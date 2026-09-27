"""Launch a cluster of local upstream simulator instances on real ports."""
from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass

import httpx
import uvicorn

from .server import create_upstream_app


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class UpstreamNode:
    name: str
    port: int
    base_url: str
    server: uvicorn.Server
    thread: threading.Thread

    def post(self, path: str, **kwargs: object) -> httpx.Response:
        return httpx.post(self.base_url + path, timeout=5, **kwargs)

    def get(self, path: str, **kwargs: object) -> httpx.Response:
        return httpx.get(self.base_url + path, timeout=5, **kwargs)


def start_node(name: str) -> UpstreamNode:
    port = _free_port()
    app = create_upstream_app(name)
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="error", lifespan="off"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    _wait_ready(base_url)
    return UpstreamNode(name, port, base_url, server, thread)


def _wait_ready(base_url: str, attempts: int = 50) -> None:
    for _ in range(attempts):
        try:
            httpx.get(base_url + "/docs", timeout=1)
            return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError(f"upstream at {base_url} never became ready")


def stop_node(node: UpstreamNode) -> None:
    node.server.should_exit = True
    node.thread.join(timeout=5)
