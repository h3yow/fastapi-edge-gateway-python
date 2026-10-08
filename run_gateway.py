"""Run the gateway against two local upstream simulator instances.

* Without --config: the built-in demo rules (gateway/defaults.py) are used,
  exactly as before.
* With --config PATH: rules are loaded from the editable JSON file, and
  POST /_gateway/config/reload re-applies it at runtime without a restart.
  Inside that file, "base_url": "sim://u1" binds an upstream to the local
  simulator node u1 (started automatically).
"""
from __future__ import annotations

import sys

import uvicorn

from gateway.app import build_gateway
from gateway.configfile import ConfigValidationError, load_config_file
from gateway.defaults import build_default_config
from upstream_sim.cluster import start_node, stop_node


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--config", type=str, default=None,
                        help="path to an editable JSON rules file")
    args = parser.parse_args(argv)
    nodes = [start_node("u1"), start_node("u2")]
    sim_urls = {n.name: n.base_url for n in nodes}

    if args.config:
        try:
            loaded = load_config_file(args.config, sim_urls=sim_urls)
        except ConfigValidationError as exc:
            print(f"configuration invalid, aborting startup ({args.config}):")
            for msg in exc.errors:
                print(f"  - {msg}")
            for n in nodes:
                stop_node(n)
            return 2
        except Exception as exc:  # noqa: BLE001 - startup must report the cause
            print(f"cannot load config {args.config}: {exc}")
            for n in nodes:
                stop_node(n)
            return 2
        gateway = build_gateway(
            loaded.config,
            version=loaded.version,
            config_path=loaded.source_path,
            sim_urls=sim_urls,
            content_hash=loaded.content_hash,
            loaded_at_ms=loaded.loaded_at_ms,
        )
        print(f"Loaded configuration {loaded.version} from {args.config} "
              f"({len(loaded.config.routes)} routes, "
              f"{len(loaded.config.upstreams)} upstreams)")
    else:
        config = build_default_config(sim_urls)
        gateway = build_gateway(config)
        print("Running with built-in default rules (no --config); runtime "
              "reload needs a config file.")

    print("Upstream nodes:")
    for n in nodes:
        print(f"  {n.name} -> {n.base_url}")
    print(f"Gateway on http://127.0.0.1:{args.port}  "
          "(callers: premium, burst-demo)")
    print("Admin: GET /_gateway/config/version | "
          "POST /_gateway/config/reload")
    try:
        uvicorn.run(gateway.app, host="127.0.0.1", port=args.port,
                    log_level="warning")
    finally:
        for n in nodes:
            stop_node(n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
