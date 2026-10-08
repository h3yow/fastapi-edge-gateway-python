"""Run the gateway against two local upstream simulator instances.

Modes:
* no flags (original demo behaviour): the built-in default rules are used;
* ``--config path.json``: rules are loaded from a local, editable JSON file
  and can be reloaded at runtime via ``POST /_gateway/config/reload``.
  The file is fully validated at startup; any problem aborts startup with
  operator-readable reasons (exit code 2) and no gateway port is opened.
"""
from __future__ import annotations

import sys

import uvicorn

from gateway.app import build_gateway
from gateway.configfile import ConfigError, load_config_file
from gateway.defaults import build_default_config
from upstream_sim.cluster import start_node, stop_node


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080,
                        help="gateway listen port")
    parser.add_argument("--config", default=None,
                        help="path to a local JSON gateway config file")
    parser.add_argument("--upstream-base-port", type=int, default=19090,
                        help="bundled sim nodes listen on base+1 and base+2")
    args = parser.parse_args(argv)

    nodes = [
        start_node("u1", port=args.upstream_base_port + 1),
        start_node("u2", port=args.upstream_base_port + 2),
    ]

    initial = None
    if args.config:
        try:
            initial = load_config_file(args.config)
        except ConfigError as exc:
            print(f"config '{args.config}' rejected; gateway NOT started:",
                  file=sys.stderr)
            for reason in exc.reasons:
                print(f"  - {reason}", file=sys.stderr)
            for n in nodes:
                stop_node(n)
            return 2
        config = initial.config
        print(f"Loaded config '{args.config}' "
              f"version={initial.version or '(none)'} "
              f"sha256={initial.content_sha256[:12]} "
              f"routes={len(config.routes)} upstreams={len(config.upstreams)}")
    else:
        config = build_default_config({n.name: n.base_url for n in nodes})

    gateway = build_gateway(
        config,
        config_path=args.config,
        initial=initial,
    )
    print("Upstream nodes:")
    for n in nodes:
        print(f"  {n.name} -> {n.base_url}")
    info = gateway.manager.info()
    print(f"Gateway on http://127.0.0.1:{args.port}  "
          f"(active config: revision={info.revision} version={info.version})")
    if args.config:
        print("Reload at runtime: "
              f"curl -XPOST http://127.0.0.1:{args.port}/_gateway/config/reload")
        print("Inspect active version: "
              f"curl http://127.0.0.1:{args.port}/_gateway/config")
    else:
        print("(callers: premium, burst-demo)")
    try:
        uvicorn.run(gateway.app, host="127.0.0.1", port=args.port,
                    log_level="warning")
    finally:
        for n in nodes:
            stop_node(n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
