"""
PFO3 – Cliente distribuido con sockets TCP
Envía tareas al servidor y recupera resultados por polling.

Uso:
    python cliente.py                  # demo automática
    python cliente.py --host 127.0.0.1 --port 9000 --modo interactivo
"""

import socket
import json
import time
import argparse
import logging

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cliente")


# ── Clase Cliente ─────────────────────────────────────────────────────────────

class ClienteSocket:
    """
    Cliente TCP que se comunica con el servidor PFO3.

    Protocolo: mensajes JSON separados por '\\n'.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9000, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._conn: socket.socket | None = None
        self._buffer = b""

    # ── Conexión ──────────────────────────────────────────────────────────────

    def conectar(self) -> None:
        """Abre la conexión TCP al servidor."""
        self._conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._conn.settimeout(self.timeout)
        self._conn.connect((self.host, self.port))
        logger.info("Conectado a %s:%d", self.host, self.port)

    def desconectar(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Desconectado del servidor.")

    def __enter__(self):
        self.conectar()
        return self

    def __exit__(self, *_):
        self.desconectar()

    # ── I/O ───────────────────────────────────────────────────────────────────

    def _enviar(self, msg: dict) -> dict:
        """Envía un mensaje JSON y espera la respuesta."""
        if self._conn is None:
            raise RuntimeError("No hay conexión activa. Llame a conectar() primero.")

        datos = (json.dumps(msg) + "\n").encode()
        self._conn.sendall(datos)

        # Leer hasta recibir un '\n'
        while b"\n" not in self._buffer:
            chunk = self._conn.recv(4096)
            if not chunk:
                raise ConnectionError("El servidor cerró la conexión.")
            self._buffer += chunk

        linea, self._buffer = self._buffer.split(b"\n", 1)
        return json.loads(linea.decode())

    # ── API pública ───────────────────────────────────────────────────────────

    def ping(self) -> dict:
        """Verifica la conexión con el servidor."""
        return self._enviar({"accion": "ping"})

    def enviar_tarea(self, tipo: str, datos: dict) -> str:
        """
        Encola una tarea en el servidor.

        Returns:
            tarea_id asignado por el servidor.
        """
        respuesta = self._enviar({
            "accion": "enviar_tarea",
            "tarea": {"tipo": tipo, "datos": datos},
        })
        if not respuesta.get("ok"):
            raise RuntimeError(f"Error al enviar tarea: {respuesta}")
        tarea_id = respuesta["tarea_id"]
        logger.info("Tarea enviada → id=%s  tipo=%s", tarea_id, tipo)
        return tarea_id

    def obtener_resultado(self, tarea_id: str) -> dict | None:
        """
        Consulta el resultado de una tarea.

        Returns:
            dict con el resultado o None si aún está pendiente.
        """
        respuesta = self._enviar({"accion": "obtener_resultado", "tarea_id": tarea_id})
        if respuesta.get("ok") and "resultado" in respuesta:
            return respuesta
        return None

    def esperar_resultado(self, tarea_id: str, intervalo: float = 0.3,
                          max_espera: float = 30.0) -> dict:
        """
        Polling hasta obtener el resultado o agotar el tiempo máximo.

        Returns:
            dict con el resultado completo.

        Raises:
            TimeoutError si max_espera se agota.
        """
        inicio = time.time()
        while True:
            resultado = self.obtener_resultado(tarea_id)
            if resultado:
                return resultado
            if time.time() - inicio > max_espera:
                raise TimeoutError(f"Tiempo agotado esperando resultado de {tarea_id}")
            time.sleep(intervalo)

    def listar_tareas(self) -> list[dict]:
        """Devuelve todas las tareas ya procesadas."""
        respuesta = self._enviar({"accion": "listar_tareas"})
        return respuesta.get("tareas", [])


# ── Demo automática ───────────────────────────────────────────────────────────

def demo(host: str, port: int) -> None:
    """Ejecuta un conjunto de tareas de ejemplo y muestra los resultados."""
    tareas_demo = [
        ("suma",       {"numeros": [10, 20, 30, 40]}),
        ("eco",        {"mensaje": "Hola desde el cliente PFO3"}),
        ("mayusculas", {"texto": "arquitectura distribuida"}),
        ("invertir",   {"texto": "Python con sockets"}),
        ("suma",       {"numeros": list(range(1, 11))}),
    ]

    print("\n" + "=" * 60)
    print("  PFO3 – Demo de cliente distribuido")
    print("=" * 60)

    with ClienteSocket(host, port) as cliente:
        # Ping inicial
        pong = cliente.ping()
        print(f"\n[PING] Servidor responde: {pong}\n")

        ids_tareas = []

        # Enviar todas las tareas
        print("── Enviando tareas ──────────────────────────────────────")
        for tipo, datos in tareas_demo:
            tarea_id = cliente.enviar_tarea(tipo, datos)
            ids_tareas.append((tarea_id, tipo))
            print(f"  ✓ {tipo:<12} → id={tarea_id[:8]}…")

        print(f"\n── Esperando resultados ({len(ids_tareas)} tareas) ──────────────")
        time.sleep(0.5)  # pequeña pausa para que los workers arranquen

        for tarea_id, tipo in ids_tareas:
            try:
                res = cliente.esperar_resultado(tarea_id, intervalo=0.2, max_espera=15)
                worker = res.get("worker_id", "?")
                duracion = res.get("duracion_s", "?")
                resultado = res.get("resultado", {})
                print(f"\n  [{tipo.upper()}]")
                print(f"    id       : {tarea_id[:8]}…")
                print(f"    worker   : {worker}")
                print(f"    duración : {duracion}s")
                print(f"    resultado: {resultado}")
            except TimeoutError:
                print(f"  ✗ Timeout en tarea {tarea_id[:8]}…")

        # Listar todas
        print("\n── Resumen de tareas completadas ────────────────────────")
        todas = cliente.listar_tareas()
        for t in todas:
            print(f"  • {t['tarea_id'][:8]}…  worker={t['worker_id']}  {t['duracion_s']}s")

    print("\n" + "=" * 60 + "\n")


# ── Modo interactivo ──────────────────────────────────────────────────────────

def interactivo(host: str, port: int) -> None:
    """Shell REPL mínimo para enviar tareas manualmente."""
    print("\nModo interactivo – escribe 'ayuda' para ver los comandos.")
    with ClienteSocket(host, port) as cliente:
        while True:
            try:
                entrada = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo.")
                break

            if not entrada:
                continue

            partes = entrada.split(None, 1)
            cmd = partes[0].lower()

            if cmd in ("salir", "exit", "quit"):
                break

            elif cmd == "ayuda":
                print(
                    "\n  ping                         – verifica el servidor\n"
                    "  enviar <tipo> <datos_json>   – encola una tarea\n"
                    "  resultado <tarea_id>         – consulta un resultado\n"
                    "  listar                       – muestra todas las tareas\n"
                    "  salir                        – cierra el cliente\n\n"
                    "  Tipos disponibles: suma, eco, mayusculas, invertir, espera\n"
                    "  Ejemplo: enviar suma {\"numeros\":[1,2,3]}\n"
                )

            elif cmd == "ping":
                print(cliente.ping())

            elif cmd == "enviar" and len(partes) == 2:
                sub = partes[1].split(None, 1)
                if len(sub) < 2:
                    print("Uso: enviar <tipo> <datos_json>")
                    continue
                tipo, datos_raw = sub[0], sub[1]
                try:
                    datos = json.loads(datos_raw)
                    tarea_id = cliente.enviar_tarea(tipo, datos)
                    print(f"Tarea encolada → {tarea_id}")
                except json.JSONDecodeError:
                    print("Error: datos no son JSON válido.")

            elif cmd == "resultado" and len(partes) == 2:
                tarea_id = partes[1].strip()
                res = cliente.obtener_resultado(tarea_id)
                if res:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print("Resultado pendiente o id no encontrado.")

            elif cmd == "listar":
                tareas = cliente.listar_tareas()
                if not tareas:
                    print("Sin tareas completadas aún.")
                for t in tareas:
                    print(json.dumps(t, indent=2, ensure_ascii=False))

            else:
                print("Comando no reconocido. Escribe 'ayuda'.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PFO3 – Cliente distribuido")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--modo",
        choices=["demo", "interactivo"],
        default="demo",
        help="'demo' ejecuta tareas de ejemplo; 'interactivo' abre un shell",
    )
    args = parser.parse_args()

    if args.modo == "interactivo":
        interactivo(args.host, args.port)
    else:
        demo(args.host, args.port)
