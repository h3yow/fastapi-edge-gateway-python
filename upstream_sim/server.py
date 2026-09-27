"""Local upstream simulator used to mimic a backend cluster.

Each instance is one upstream node and exposes:
* GET  /echo/<path>  -> node identity, headers, query (safe, cacheable)
* POST /side-effect  -> increments a per-node counter exactly once per call
* GET  /slow?ms=...  -> delayed response (timeout tests)
* GET  /fail/<code>  -> returns the given status (502/503/504 failover tests)
* admin control endpoints to freeze / resume the node.
"""
from __future__ import annotations

import threading
import time

from fastapi import FastAPI, Request

NODE_NAME_HEADER = "X-Upstream-Node"


def create_upstream_app(node_name: str) -> FastAPI:
    app = FastAPI(title=f"upstream-{node_name}")
    lock = threading.Lock()
    state = {"frozen": False, "side_effects": 0, "requests": 0}

    @app.middleware("http")
    async def tag_node(request: Request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        response.headers[NODE_NAME_HEADER] = node_name
        return response

    def _freeze_response() -> tuple[int, bytes] | None:
        # A frozen node behaves like an unreachable instance: hold briefly then
        # answer 503 so the gateway sees a retriable failure.
        if state["frozen"]:
            time.sleep(0.05)
            return 503, b'{"error":"node_frozen"}'
        return None

    @app.post("/__admin/freeze")
    def freeze() -> dict[str, bool]:
        with lock:
            state["frozen"] = True
        return {"frozen": True}

    @app.post("/__admin/resume")
    def resume() -> dict[str, bool]:
        with lock:
            state["frozen"] = False
        return {"frozen": False}

    @app.post("/__admin/reset")
    def reset() -> dict[str, int]:
        with lock:
            state["side_effects"] = 0
            state["requests"] = 0
        return {"side_effects": 0, "requests": 0}

    @app.get("/__admin/state")
    def get_state() -> dict[str, object]:
        with lock:
            return dict(state, node=node_name)

    @app.api_route("/echo/{full_path:path}",
                   methods=["GET", "HEAD", "POST"])
    async def echo(request: Request, full_path: str) -> dict[str, object]:
        with lock:
            state["requests"] += 1
        frozen = _freeze_response()
        if frozen is not None:
            from fastapi.responses import Response
            return Response(frozen[1], status_code=frozen[0],
                            media_type="application/json")
        raw = await request.body()
        return {
            "node": node_name,
            "path": "/" + full_path,
            "query": str(request.url.query),
            "caller": request.headers.get("x-caller-id", "anonymous"),
            "body_len": len(raw),
        }

    @app.post("/side-effect")
    async def side_effect(request: Request) -> dict[str, object]:
        with lock:
            state["requests"] += 1
            state["side_effects"] += 1
            count = state["side_effects"]
        frozen = _freeze_response()
        if frozen is not None:
            from fastapi.responses import Response
            return Response(frozen[1], status_code=frozen[0],
                            media_type="application/json")
        return {"node": node_name, "side_effect_seq": count}

    @app.get("/slow")
    def slow(ms: int = 2000) -> dict[str, object]:
        with lock:
            state["requests"] += 1
        frozen = _freeze_response()
        if frozen is not None:
            from fastapi.responses import Response
            return Response(frozen[1], status_code=frozen[0],
                            media_type="application/json")
        time.sleep(ms / 1000.0)
        return {"node": node_name, "slept_ms": ms}

    @app.get("/hold")
    def hold(ms: int = 400) -> dict[str, object]:
        """Sleep then succeed: used for in-flight concurrency tests."""
        with lock:
            state["requests"] += 1
            frozen = _freeze_response()
        if frozen is not None:
            from fastapi.responses import Response
            return Response(frozen[1], status_code=frozen[0],
                            media_type="application/json")
        time.sleep(ms / 1000.0)
        return {"node": node_name, "held_ms": ms}

    @app.get("/fail/{code}")
    def fail(code: int) -> dict[str, object]:
        with lock:
            state["requests"] += 1
        frozen = _freeze_response()
        if frozen is not None:
            from fastapi.responses import Response
            return Response(frozen[1], status_code=frozen[0],
                            media_type="application/json")
        from fastapi.responses import Response
        return Response(b'{"error":"forced"}', status_code=code,
                        media_type="application/json")

    return app
