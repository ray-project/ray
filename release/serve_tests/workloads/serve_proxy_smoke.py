import time
import socket
import signal
import requests
import subprocess
import uvicorn
from fastapi import FastAPI
import threading

# -------------------
# FastAPI Application
# -------------------
app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI via HAProxy!"}


def get_free_port() -> int:
    """Find a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# -------------------
# HAProxy Config
# -------------------
def generate_haproxy_cfg(backend_host, backend_port, include_backend=True):
    backend_block = ""
    if include_backend:
        backend_block = f"""
backend fastapi_backend
    server fastapi {backend_host}:{backend_port} check
"""
    return f"""
global
    daemon
    maxconn 256

defaults
    mode http
    timeout connect 5s
    timeout client  30s
    timeout server  30s

frontend http_front
    bind *:8000
    default_backend fastapi_backend

{backend_block}
"""


def write_cfg(cfg_text):
    with open("haproxy.cfg", "w") as f:
        f.write(cfg_text)


def start_haproxy():
    return subprocess.Popen(["haproxy", "-f", "haproxy.cfg"])


def stop_haproxy(proc):
    proc.send_signal(signal.SIGTERM)
    proc.wait()


# -------------------
# FastAPI Background
# -------------------
def start_fastapi(host, port):
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    host = "127.0.0.1"
    fastapi_port = get_free_port()

    threading.Thread(
        target=start_fastapi, args=(host, fastapi_port), daemon=True
    ).start()
    time.sleep(2)

    print(f"FastAPI running on {host}:{fastapi_port}")

    # Write config WITH backend
    write_cfg(generate_haproxy_cfg(host, fastapi_port, include_backend=True))

    haproxy_proc = start_haproxy()
    time.sleep(1)

    # Validate request through HAProxy
    resp = requests.get("http://localhost:8000")
    assert resp.status_code == 200
    print("HAProxy proxying OK:", resp.json())

    # Reload config WITHOUT backend
    write_cfg(generate_haproxy_cfg(host, fastapi_port, include_backend=False))
    haproxy_proc.send_signal(signal.SIGHUP)  # reload
    time.sleep(1)

    try:
        requests.get("http://localhost:8000", timeout=3)
        raise RuntimeError("Expected failure but got success")
    except Exception as e:
        print("Request failed after removing backend:", e)

    # Shutdown HAProxy
    stop_haproxy(haproxy_proc)
    print("HAProxy stopped")
