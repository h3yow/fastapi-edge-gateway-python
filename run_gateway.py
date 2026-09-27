"""Run the gateway against two local upstream simulator instances."""
from __future__ import annotations

import sys

import uvicorn

from gateway.app import build_gateway
from gateway.defaults import build_default_config
from upstream_sim.cluster import start_node, stop_node


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    nodes = [start_node("u1"), start_node("u2")]
    config = build_default_config({n.name: n.base_url for n in nodes})
    gateway = build_gateway(config)
    print("Upstream nodes:")
    for n in nodes:
        print(f"  {n.name} -> {n.base_url}")
    print(f"Gateway on http://127.0.0.1:{args.port}  "
          "(callers: premium, burst-demo)")
    try:
        uvicorn.run(gateway.app, host="127.0.0.1", port=args.port,
                    log_level="warning")
    finally:
        for n in nodes:
            stop_node(n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
