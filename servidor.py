"""
PFO3 – Servidor distribuido con sockets TCP
Arquitectura: socket server → pool de hilos → cola interna → workers

Uso:
    python servidor.py            # arranca en 127.0.0.1:9000
    python servidor.py 0.0.0.0 9000
"""

import socket
import threading
import json
import time
import logging
import argparse
import queue
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# ── Configuración de logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s – %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("servidor")

# ── Cola global de tareas (simula RabbitMQ) ───────────────────────────────────
cola_tareas: queue.Queue = queue.Queue()

# ── Registro de resultados (simula PostgreSQL) ────────────────────────────────
resultados: dict[str, dict] = {}
lock_resultados = threading.Lock()


# ── Procesadores de tareas ────────────────────────────────────────────────────

PROCESADORES = {
    "suma": lambda datos: {"resultado": sum(datos.get("numeros", []))},
    "eco": lambda datos: {"eco": datos.get("mensaje", "")},
    "mayusculas": lambda datos: {"texto": datos.get("texto", "").upper()},
    "invertir": lambda datos: {"texto": datos.get("texto", "")[::-1]},
    "espera": lambda datos: (time.sleep(datos.get("segundos", 1)), {"ok": True})[1],
}


def procesar_tarea(tarea: dict) -> dict:
    """Ejecuta la lógica de negocio de una tarea."""
    tipo = tarea.get("tipo", "eco")
    datos = tarea.get("datos", {})
    procesador = PROCESADORES.get(tipo)

    if procesador is None:
        return {"error": f"Tipo de tarea desconocido: '{tipo}'"}

    try:
        return procesador(datos)
    except Exception as exc:
        return {"error": str(exc)}


# ── Worker que consume la cola ────────────────────────────────────────────────

def worker_loop(worker_id: int) -> None:
    """Bucle infinito: toma tareas de la cola y guarda el resultado."""
    logger.info("Worker %d iniciado", worker_id)
    while True:
        try:
            tarea_id, tarea = cola_tareas.get(timeout=1)
        except queue.Empty:
            continue

        logger.info("Worker %d procesando tarea %s (tipo=%s)", worker_id, tarea_id, tarea.get("tipo"))
        inicio = time.time()
        resultado = procesar_tarea(tarea)
        duracion = round(time.time() - inicio, 4)

        with lock_resultados:
            resultados[tarea_id] = {
                "tarea_id": tarea_id,
                "resultado": resultado,
                "worker_id": worker_id,
                "duracion_s": duracion,
                "timestamp": datetime.utcnow().isoformat(),
            }

        cola_tareas.task_done()
        logger.info("Worker %d completó %s en %.4fs", worker_id, tarea_id, duracion)


# ── Manejador de conexiones de cliente ────────────────────────────────────────

def manejar_cliente(conn: socket.socket, addr: tuple, executor: ThreadPoolExecutor) -> None:
    """Atiende un cliente en un hilo separado."""
    logger.info("Conexión aceptada desde %s:%d", *addr)
    with conn:
        buffer = b""
        while True:
            try:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer += chunk

                # Los mensajes terminan con '\n'
                while b"\n" in buffer:
                    linea, buffer = buffer.split(b"\n", 1)
                    if not linea.strip():
                        continue
                    respuesta = _procesar_mensaje(linea, executor)
                    conn.sendall((json.dumps(respuesta) + "\n").encode())

            except (ConnectionResetError, BrokenPipeError):
                break
            except json.JSONDecodeError:
                conn.sendall((json.dumps({"error": "JSON inválido"}) + "\n").encode())

    logger.info("Conexión cerrada: %s:%d", *addr)


def _procesar_mensaje(raw: bytes, executor: ThreadPoolExecutor) -> dict:
    """Deserializa el mensaje y despacha al handler correcto."""
    msg = json.loads(raw.decode())
    accion = msg.get("accion", "")

    if accion == "enviar_tarea":
        tarea_id = str(uuid.uuid4())
        cola_tareas.put((tarea_id, msg.get("tarea", {})))
        logger.info("Tarea encolada: %s", tarea_id)
        return {"ok": True, "tarea_id": tarea_id, "estado": "encolada"}

    if accion == "obtener_resultado":
        tarea_id = msg.get("tarea_id", "")
        with lock_resultados:
            res = resultados.get(tarea_id)
        if res:
            return {"ok": True, **res}
        return {"ok": False, "estado": "pendiente", "tarea_id": tarea_id}

    if accion == "listar_tareas":
        with lock_resultados:
            lista = list(resultados.values())
        return {"ok": True, "tareas": lista, "total": len(lista)}

    if accion == "ping":
        return {"ok": True, "pong": True, "timestamp": datetime.utcnow().isoformat()}

    return {"error": f"Acción desconocida: '{accion}'"}


# ── Servidor principal ────────────────────────────────────────────────────────

def iniciar_servidor(host: str = "127.0.0.1", port: int = 9000,
                     num_workers: int = 3, max_threads: int = 10) -> None:
    """
    Arranca:
      - N workers que consumen la cola de tareas.
      - Un ThreadPoolExecutor para las conexiones entrantes.
      - Un socket TCP que acepta clientes.
    """
    # Iniciar workers de la cola
    for i in range(1, num_workers + 1):
        t = threading.Thread(target=worker_loop, args=(i,), daemon=True,
                             name=f"Worker-{i}")
        t.start()

    logger.info("Servidor escuchando en %s:%d (workers=%d, max_threads=%d)",
                host, port, num_workers, max_threads)

    with ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix="Conn") as executor:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((host, port))
            srv.listen(50)

            try:
                while True:
                    conn, addr = srv.accept()
                    executor.submit(manejar_cliente, conn, addr, executor)
            except KeyboardInterrupt:
                logger.info("Servidor detenido por el usuario.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PFO3 – Servidor distribuido")
    parser.add_argument("host", nargs="?", default="127.0.0.1")
    parser.add_argument("port", nargs="?", type=int, default=9000)
    parser.add_argument("--workers", type=int, default=3, help="Número de workers de cola")
    parser.add_argument("--threads", type=int, default=10, help="Máx. hilos de conexión")
    args = parser.parse_args()

    iniciar_servidor(args.host, args.port, args.workers, args.threads)
