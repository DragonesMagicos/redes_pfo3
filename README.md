# PFO3 — Rediseño como Sistema Distribuido (Cliente-Servidor)

API de gestión de tareas basada en **sockets TCP** con arquitectura distribuida multi-hilo.  
Evolución del PFO2 (Flask + SQLite) hacia un sistema con workers, cola de mensajes y almacenamiento distribuido.

---

## Arquitectura

```
Clientes (móvil / web / Python)
          │  TCP socket
          ▼
  Balanceador de carga (Nginx / HAProxy)
          │  Round-robin
          ▼
  ┌──────────────────────────────────────────┐
  │  Servidor Python                         │
  │  ThreadPoolExecutor ──► manejar_cliente  │
  └──────────────┬───────────────────────────┘
                 │  queue.Queue  (simula RabbitMQ)
       ┌─────────┼─────────┐
       ▼         ▼         ▼
   Worker 1  Worker 2  Worker 3   ← ThreadPoolExecutor por worker
       │         │         │
       └─────────┼─────────┘
                 │  dict en memoria (simula PostgreSQL)
                 ▼
         Resultados / logs  →  S3 (archivos grandes)
```

### Componentes

| Capa | Tecnología real | Implementación en este proyecto |
|---|---|---|
| Clientes | iOS, Android, navegador | `cliente.py` con socket TCP |
| Load balancer | Nginx / HAProxy | descripción en diagrama |
| Servidor | Python sockets | `servidor.py` |
| Pool de hilos | `ThreadPoolExecutor` | sí, implementado |
| Cola de mensajes | RabbitMQ (AMQP) | `queue.Queue` |
| Base de datos | PostgreSQL | `dict` con `threading.Lock` |
| Almacenamiento | Amazon S3 | referenciado en arquitectura |

---

## Requisitos

```bash
Python 3.10+   # sólo stdlib, sin dependencias externas
```

---

## Instalación y ejecución

```bash
# Clonar el repositorio
git clone https://github.com/DragonesMagicos/redes_pfo3
cd redes_pfo3

# 1. Iniciar el servidor (terminal A)
python servidor.py

# Opciones avanzadas
python servidor.py 0.0.0.0 9000 --workers 5 --threads 20

# 2. Ejecutar el cliente demo (terminal B)
python cliente.py

# Modo interactivo
python cliente.py --modo interactivo
```

---

## Protocolo de comunicación

Mensajes JSON delimitados por `\n`.

### Enviar tarea

**Request**
```json
{"accion": "enviar_tarea", "tarea": {"tipo": "suma", "datos": {"numeros": [1,2,3]}}}
```

**Response**
```json
{"ok": true, "tarea_id": "uuid-…", "estado": "encolada"}
```

### Obtener resultado

**Request**
```json
{"accion": "obtener_resultado", "tarea_id": "uuid-…"}
```

**Response (listo)**
```json
{
  "ok": true,
  "tarea_id": "uuid-…",
  "resultado": {"resultado": 6},
  "worker_id": 2,
  "duracion_s": 0.0003,
  "timestamp": "2025-06-01T12:00:00"
}
```

**Response (pendiente)**
```json
{"ok": false, "estado": "pendiente", "tarea_id": "uuid-…"}
```

### Listar tareas completadas

```json
{"accion": "listar_tareas"}
```

### Ping

```json
{"accion": "ping"}
```

---

## Tipos de tarea disponibles

| Tipo | Datos requeridos | Resultado |
|---|---|---|
| `suma` | `{"numeros": [1,2,3]}` | `{"resultado": 6}` |
| `eco` | `{"mensaje": "hola"}` | `{"eco": "hola"}` |
| `mayusculas` | `{"texto": "abc"}` | `{"texto": "ABC"}` |
| `invertir` | `{"texto": "abc"}` | `{"texto": "cba"}` |
| `espera` | `{"segundos": 2}` | `{"ok": true}` |

---

## Diferencias con PFO2

| Característica | PFO2 | PFO3 |
|---|---|---|
| Transporte | HTTP (Flask) | TCP socket |
| Concurrencia | Single thread Flask | ThreadPoolExecutor |
| Procesamiento | Síncrono | Asíncrono con cola |
| Almacenamiento | SQLite | Dict + lock (PostgreSQL en prod) |
| Escalabilidad | Vertical | Horizontal (múltiples workers) |
| Protocolo | REST JSON | Socket JSON delimitado por `\n` |

---

## Cómo probarlo con `netcat`

```bash
# Enviar tarea
echo '{"accion":"enviar_tarea","tarea":{"tipo":"suma","datos":{"numeros":[10,20,30]}}}' | nc 127.0.0.1 9000

# Ping
echo '{"accion":"ping"}' | nc 127.0.0.1 9000
```

---

## Autores

Equipo Dragones Mágicos — Materia: Redes de Computadoras
