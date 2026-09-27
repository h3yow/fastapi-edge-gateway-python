"""模拟上游实例：可启动多个进程共同扮演一个后端集群。

用法：
    python upstream_app.py --name orders-1 --port 9001

能力：
    GET  /<任意路径>            普通响应，回显上游名与请求序号
        ?slow=<秒>             仅名称以 "slow-" 开头的实例延迟响应（模拟单实例慢故障）
        ?hold=<秒>             所有实例延迟响应（模拟处理耗时，用于并发测试）
        路径以 /<上游名>/fail 结尾
                                仅名称匹配的实例返回 503（模拟单实例故障，
                                如 orders-1/fail 只让 orders-1 失败）
        路径以 /cluster-fail 结尾
                                所有实例都返回 503（模拟集群整体故障，不记录副作用）
    POST/PUT/PATCH/DELETE       记录一次副作用（side effect 计数），返回 201
    GET  /__diag/state         查看请求总数与副作用计数
    GET  /__diag/reset         清零计数
"""
from __future__ import annotations

import argparse
import itertools

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def create_upstream_app(name: str) -> FastAPI:
    """每个上游实例拥有独立 app 与独立统计计数器。"""
    app = FastAPI(title=f"demo-upstream-{name}")
    app.state.upstream_name = name
    counter = itertools.count(1)
    stats = {"requests_total": 0, "side_effects": {}}

    @app.get("/__diag/state")
    async def diag_state() -> dict:
        return {"upstream": name, **stats}

    @app.get("/__diag/reset")
    async def diag_reset() -> dict:
        stats["requests_total"] = 0
        stats["side_effects"].clear()
        return {"status": "reset", "upstream": name}

    @app.api_route(
        "/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
    )
    async def catch_all(request: Request, full_path: str = "") -> JSONResponse:
        stats["requests_total"] += 1
        seq = next(counter)
        path = "/" + full_path

        # 可重试错误注入：
        # - 路径以 /<本实例名>/fail 结尾：仅本实例失败（模拟单实例故障）；
        # - 路径以 /cluster-fail 结尾：所有实例失败（模拟集群整体故障）。
        # 均不记录副作用。
        if full_path.endswith(f"{name}/fail") or full_path.endswith("cluster-fail"):
            return JSONResponse(
                status_code=503,
                content={"upstream": name, "seq": seq, "error": "injected failure"},
            )

        if request.method == "GET":
            slow = request.query_params.get("slow")
            if slow and name.startswith("slow-"):
                import asyncio
                await asyncio.sleep(float(slow))
            hold = request.query_params.get("hold")
            if hold:
                import asyncio
                await asyncio.sleep(float(hold))
            return JSONResponse(
                {
                    "upstream": name,
                    "seq": seq,
                    "method": request.method,
                    "path": path,
                    "query": request.url.query,
                }
            )

        # 写操作：记录副作用（用于验证非安全请求不会被重试）
        effects = stats["side_effects"]
        effects[path] = effects.get(path, 0) + 1
        body = await request.body()
        return JSONResponse(
            status_code=201,
            content={
                "upstream": name,
                "seq": seq,
                "method": request.method,
                "path": path,
                "side_effect_count": effects[path],
                "received": body.decode("utf-8", errors="replace"),
            },
        )

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    uvicorn.run(
        create_upstream_app(args.name),
        host=args.host,
        port=args.port,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
