# FastAPI 边缘网关（本地可运行）

一套本地可运行的 API 边缘网关：基于配置的动态路由、多上游轮询分发、
按调用方维度的令牌桶限流与并发约束、超时/重试/故障转移，以及响应缓存。
后端集群由本地启动的多个上游实例模拟。

所有判定结论都**可解释、可区分**：

| 响应头 / 字段 | 含义 |
| --- | --- |
| `X-Gateway-Reason`（仅失败） | 拒绝/失败原因码，见下表 |
| `X-Gateway-Upstream` | 实际命中的上游名；缓存命中时为 `cache` |
| `X-Gateway-Attempts` | 本次请求的尝试次数（缓存命中为 0） |
| `X-Gateway-Cache` | `HIT` / `MISS` 缓存标记 |

失败原因码（HTTP 状态码 + JSON body `{"error": ..., "detail": ...}`）：

| error 原因码 | 状态码 | 触发场景 |
| --- | --- | --- |
| `route_not_found` | 404 | 路径不匹配任何路由前缀 |
| `rate_limited` | 429 | 调用方令牌桶无可用令牌 |
| `concurrency_limited` | 429 | 调用方在途请求达到并发上限 |
| `no_healthy_upstream` | 503 | 路由存在但**尚无尝试**即无健康上游（全部被剔除） |
| `upstream_timeout` | 504 | 上游超时且重试/故障转移耗尽 |
| `upstream_error` | 502/503 | 连接失败或上游返回 5xx，重试耗尽 |

限流拒绝（429）发生在转发之前，与上游自身失败（5xx）天然可区分；
`no_healthy_upstream` 与 `upstream_error` 也互不等价：
前者是“没有可尝试的健康实例”，后者是“尝试过但失败了”。

## 目录结构

```
gateway/
  config.py     配置模型（路由/上游/限流/缓存/重试）
  errors.py     可区分的错误原因码
  router.py     最长前缀路由匹配
  upstream.py   上游池：轮询选择 + 熔断剔除 + 半开探测恢复
  ratelimit.py  每调用方令牌桶 + 并发计数
  cache.py      TTL 响应缓存（键区分调用方与请求变体）
  proxy.py      转发引擎：缓存 → 转发 → 超时/重试/故障转移
  app.py        FastAPI 装配、通配转发端点、动态重载管理接口
main.py         网关启动入口
upstream_app.py 模拟上游实例（可注入慢响应/5xx/副作用计数）
tests/          22 个测试（真实 uvicorn 上游 + ASGI 网关）
config.json     示例配置
```

## 配置说明

```jsonc
{
  "routes": [
    {
      "path_prefix": "/api/orders",       // 路径前缀，最长前缀优先匹配
      "upstreams": [                      // 多个上游，按轮询分发
        {"name": "orders-1", "base_url": "http://127.0.0.1:9101"},
        {"name": "orders-2", "base_url": "http://127.0.0.1:9102"}
      ],
      "rate_limit": {
        "rate_per_second": 5.0,           // 令牌补充速率（每调用方）
        "burst": 10,                      // 桶容量（突发上限），初始满桶
        "max_concurrency": 5              // 每调用方最大在途并发
      },
      "cache": {
        "enabled": true,
        "ttl_seconds": 30.0,
        "max_entries": 128
      },
      "retry": {
        "max_attempts": 2,                // 最多尝试次数（仅安全请求可重试）
        "timeout_seconds": 2.0,           // 单次上游调用超时
        "health_cooldown": 0.3            // 不健康上游的恢复探测冷却（秒）
      }
    }
  ]
}
```

## 路由与上游选择策略

1. **最长前缀匹配**：`/api/orders/x` 优先命中 `/api/orders` 而非 `/api`；
   无匹配直接 404 `route_not_found`，绝不静默转发到错误目标。
2. **轮询（round-robin）**：仅在健康上游之间轮询。
3. **健康状态变化**：
   - 一次连接失败 / 超时 / 可重试 5xx（500/502/503/504）即**熔断剔除**；
   - 冷却 `health_cooldown` 秒后放行**一次半开探测**：成功立即恢复并
     重新纳入轮询，失败则重新冷却（同一上游同时只允许一个探测请求）。
4. **全部不可用**：未尝试即无健康实例时 503 `no_healthy_upstream`。

## 限流与并发策略

- 调用方由请求头 `X-Caller-Id` 标识（缺省为 `anonymous`），每个调用方、
  每条路由拥有独立的令牌桶与并发计数，互不影响。
- 令牌桶：容量 `burst`，按 `rate_per_second` 持续补充。取令牌是无阻塞的
  临界区，突发并发下**不多发、不漏计**。
- 并发：达到 `max_concurrency` 立即 429 `concurrency_limited`（不排队）；
  请求结束（含上游失败）一定归还槽位。
- 判定顺序：路由 → 令牌桶（429 `rate_limited`）→ 并发槽（429
  `concurrency_limited`）→ 缓存 → 上游转发。被限流的请求不会到达上游。

## 超时、重试、故障转移与缓存策略

- 仅**安全方法**（GET/HEAD/OPTIONS）允许重试与故障转移；POST/PUT/PATCH/
  DELETE 只尝试一次，失败即返回（detail 中注明“非安全方法，未重放”），
  **不会产生重复副作用**。
- 每次尝试失败后在其余健康上游间转移；尝试次数耗尽返回
  `upstream_error` / `upstream_timeout`。
- 缓存：
  - 仅缓存安全方法的 **2xx** 响应；5xx、写操作均不缓存；
  - 缓存键 = `调用方 | 方法 | 路径 | 查询串 | 请求体SHA-256`，
    不同调用方互相隔离，query/body 任一不同即为不同变体；
  - 命中返回 `X-Gateway-Cache: HIT`、`X-Gateway-Upstream: cache`、
    `X-Gateway-Attempts: 0`，且不占用上游并发槽；
  - TTL 到期或无条目为 MISS，日志记录 `expired` / `no_entry` 依据。

## 动态配置重载（运行时）

- `POST /__admin/reload`：body 为完整的新配置，立即重建路由表
  （各前缀已有的限流桶/缓存按前缀保留）；
- `GET /__admin/config`：查看当前配置。

## 本地运行

```bash
# 1) 启动两个模拟上游（模拟一个后端集群）
uv run python upstream_app.py --name orders-1 --port 9101 &
uv run python upstream_app.py --name orders-2 --port 9102 &

# 2) 启动网关（按需把 config.json 中端口改为 9101/9102）
uv run python main.py --config config.json --port 8180
```

模拟上游的故障注入能力：

| 请求 | 行为 |
| --- | --- |
| `GET /任意路径` | 正常响应，回显上游名与请求序号 |
| `GET /路径?hold=0.3` | 所有实例延迟 0.3s（并发测试） |
| `GET /路径?slow=2` | 仅名称以 `slow-` 开头的实例延迟 2s（单实例慢故障） |
| `*  /.../cluster-fail` | 所有实例返回 503（集群整体故障，不记录副作用） |
| `*  /.../<上游名>/fail` | 仅同名实例返回 503（单实例故障） |
| `POST/PUT/PATCH/DELETE /任意路径` | 201 并对该路径累加副作用计数 |
| `GET /__diag/state`、`/reset` | 查看/清零该实例的请求总数与副作用计数 |

## 验证方法

### 自动化测试

```bash
uv run pytest -q
```

22 个用例全部基于**真实网络链路**（线程内 uvicorn 上游 + httpx ASGI 网关），
`pytest.ini` 开启了 `log_cli`，运行日志会打印每一步判定依据：

- 上游选择：`upstream select: chose=... healthy=[...]` /
  `probing recovering upstream=... (half-open)`；
- 限流判定：`rate-limit caller=... decision=ALLOW/REJECT remaining=...`、
  `concurrency caller=... decision=... inflight=N/M`；
- 缓存决策：`cache key ...`、`cache decision=HIT/MISS/STORE ... reason=...`。

覆盖场景：

- **上游不可用**：单实例 5xx 当次请求内故障转移；慢实例超时剔除；
  连接被拒（502）；集群整体故障时 `upstream_error` 与随后的
  `no_healthy_upstream` 可区分；冷却后探测恢复并重新纳入轮询；
  非安全请求失败不重试、副作用不重复。
- **突发限流**：零补充速率下满桶突发精确放行（12 并发恰好 5 放行 7 拒绝）；
  令牌随时间补充；不同调用方桶隔离；并发上限精确（5 并发、上限 2）；
  上游失败后并发槽正确归还；限流请求不到达上游。
- **缓存**：HIT 标记与上游零流量；TTL 过期重新转发；调用方隔离；
  query 变体区分；POST 不缓存；5xx 不缓存；关闭缓存始终 MISS。

### 手工冒烟

```bash
curl -D - -H 'X-Caller-Id: demo' http://127.0.0.1:8180/api/orders/orders-1/fail
# 观察 X-Gateway-Upstream: orders-2、X-Gateway-Attempts: 2（故障转移）

curl -D - -H 'X-Caller-Id: demo' http://127.0.0.1:8180/api/orders/sku/9
curl -D - -H 'X-Caller-Id: demo' http://127.0.0.1:8180/api/orders/sku/9
# 第一次 X-Gateway-Cache: MISS，第二次 HIT

for i in $(seq 1 12); do
  curl -s -o /dev/null -w '%{http_code}\n' \
    -H 'X-Caller-Id: demo' http://127.0.0.1:8180/api/orders/list
done
# burst 内 200，超出后 429（error=rate_limited）
```
