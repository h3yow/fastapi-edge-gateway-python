# FastAPI Edge Gateway（本地可运行）

一套可在本地完整运行的 API 边缘网关：用多个本地启动的 **上游模拟器实例** 模拟后端集群，
在统一入口下提供「动态路由 + 上游选择、调用方维度限流/并发约束、超时/安全重试/故障转移、
响应缓存」能力，并对每一类拒绝与命中给出**可区分、可解释**的结论。

## 1. 架构与模块

| 模块 | 职责 |
| --- | --- |
| `gateway/config.py` | 配置模型：上游、路由（前缀/策略/超时/重试/缓存 TTL）、调用方限流规则 |
| `gateway/router.py` | 健康感知的上游选择：轮询/加权随机、剔除、半开恢复探针（含错峰探针） |
| `gateway/health.py` | 每上游连续失败计数熔断；连续探针成功后自动重新纳入 |
| `gateway/ratelimit.py` | 按调用方的令牌桶限流 + 在途并发数约束（单锁，突发精确） |
| `gateway/cache.py` | 仅安全方法的响应缓存；键含调用方/路由/路径/查询/Vary 头；TTL 与失效 |
| `gateway/forwarder.py` | 转发：超时、**仅安全请求**重试、跨上游故障转移 |
| `gateway/app.py` | FastAPI 装配，统一的可区分错误码与决策响应头 |
| `upstream_sim/` | 本地后端模拟器（可冻结/恢复、可注入延迟与 5xx、副作用计数） |
| `tests/` | 单元测试 + 基于真实 HTTP 集群的端到端测试 |

## 2. 路由与上游选择规则

* 路由按 **最长路径前缀** 匹配（`config.route_for`）；未命中返回
  `404 {"error":"route_not_found"}`，响应头 `X-Gateway-Reason: route_not_found`，
  **绝不静默转发**到默认目标。
* 一条路由可配置多个上游，默认 `round_robin`（另支持加权 `random`）。
* 健康判定：连续失败达到 `fail_threshold` 立即剔除；剔除后的节点进入半开状态，
  探针冷却 `probe_interval_ms`（默认 500ms）过后才允许一次试探请求：
  * 集群尚有健康节点时，仅用 **可重放请求** 错峰试探故障节点，试探失败不影响正常请求；
  * 连续探针成功达到 `recover_threshold` 后自动重新纳入轮询；
  * 非安全方法（POST/PUT/PATCH/DELETE）在所有上游都不可用时**直接拒绝**，
    不会被拿去做可能制造重复副作用的探针。
* 全部上游不可用（且无可用探针窗口）时返回
  `503 {"error":"upstream_unavailable"}`，`X-Gateway-Reason` 为
  `all_upstreams_unhealthy` / `status_503` 等，可与限流、无路由明确区分。
* 可通过 `GET /_gateway/health` 查看各上游实时健康状态。

## 3. 限流与并发约束

* 调用方由请求头 `X-Caller-Id` 标识；缺省为 `anonymous`，走默认配额。
* 每个调用方独立维护 **令牌桶**：`rate_per_sec` 持续补充，上限 `burst`。
  令牌不足 → `429 {"error":"rate_limited"}`，头 `X-Gateway-Reason: rate_limited`。
* 每个调用方独立维护 **在途并发数** `max_concurrency`（请求结束释放）。
  超额 → `429 {"error":"concurrency_limited"}`，头同名值 `concurrency_limited`。
* 令牌预扣与并发占位在同一把锁内完成，多线程突发下不会多计或漏计
  （`tests/test_units.py::test_parallel_burst_never_over_admits` 以 300 线程验证）。
* 默认规则（`gateway/defaults.py`）：
  * `premium`：100 req/s，突发 100，并发 20；
  * `burst-demo`：2 req/s，突发 5，并发 3；
  * 其他调用方：20 req/s，突发 20，并发 10。

> 限流拒绝（429）来自网关本地；上游失败是 503/504，二者状态码、错误体、
> `X-Gateway-Reason` 均不同，调用方可以程序化区分。

## 4. 超时、重试与故障转移

* 每条路由配置 `timeout_ms`；连接失败/网络错误/读超时分别映射为
  `504` + `X-Gateway-Reason: ConnectError / ReadTimeout / ...`。
* **只有安全可重放方法**（GET/HEAD/OPTIONS）允许重试；且每次重试必须切换到
  **不同的上游**（选择器通过 `exclude` 排除已尝试节点），不会在同一节点重放。
* 触发故障转移的条件：连接/网络错误、读超时、上游 502/503/504。
* 非安全方法**只发一次**：即便上游故障也不重试，从而保证
  「不产生重复副作用」（见 `test_post_not_retried_no_duplicate_side_effect`）。
* 所有候选都失败时，返回最后一次具体失败或统一的
  `503 upstream_unavailable`，并在响应头标注服务过的上游。

## 5. 缓存策略

* 仅 `GET/HEAD` 且路由 `cache_ttl_ms > 0` 时参与缓存；只缓存 2xx 响应。
* 缓存键（SHA-256）包含：**调用方 + 路由名 + 方法 + 路径 + 查询串 + Vary 头**
  （`accept / accept-encoding / accept-language`），因此：
  * 不同调用方互不可见；
  * 不同查询参数、不同 Accept 变体分别缓存；
  * 同调用方同变体的重复请求命中同一条目。
* 响应头 `X-Cache: MISS | HIT | STORE(仅日志)`，命中时还返回 `X-Cache-Key`
  前缀与原始来源上游 `X-Upstream`，缓存命中**可解释**。
* TTL 到期后条目过期（下一次请求 MISS 并回填）；
  `POST /_gateway/cache/invalidate`，请求体 `{"caller": "...", "route": "..."}`
  （字段可省略表示通配）可主动失效。

## 6. 决策日志

所有关键判定以结构化日志输出到 stderr，单测运行时可见：

```
decision=route_lookup path=/api/echo/x route=echo verdict=match caller=premium
decision=rate_limit caller=burst-demo verdict=reject reason=rate_limited tokens=0.000 in_flight=0
decision=upstream_select route=echo picked=u2 healthy=u2 strategy=round_robin reason=healthy_candidate
decision=failover route=echo failed=u1 reason=ConnectError next_attempt=2
decision=cache key=65328d8227a3 verdict=hit ttl_ms=2000
decision=upstream_removed upstream=u1 reason="consecutive_fails>=1"
decision=upstream_recovered upstream=u1 reason="probe_success>=1"
```

## 7. 验证方法

```bash
# 依赖由 uv 管理（Python 3.14）
uv sync

# 全部测试：会真实启动两个本地上游实例（随机端口）
uv run pytest -q            # 18 个用例
uv run pytest -s            # 同时观察上游选择 / 限流 / 缓存决策日志

# 手动启动：网关 :8080（可 --port 覆盖），自动拉起 u1/u2 两个上游
uv run python run_gateway.py --port 18080

# 轮询分发
for i in 1 2 3 4; do
  curl -s -H 'X-Caller-Id: premium' \
    http://127.0.0.1:18080/api/echo/dist$i
done

# 缓存命中（第一次 MISS，第二次 HIT）
curl -si -H 'X-Caller-Id: premium' http://127.0.0.1:18080/api/echo/cached | grep -i x-cache
curl -si -H 'X-Caller-Id: premium' http://127.0.0.1:18080/api/echo/cached | grep -i x-cache

# 突发限流：burst=5，前 5 个 200，后 3 个 429 且原因可区分
for i in $(seq 1 8); do
  curl -s -o /dev/null -w '%{http_code} ' -H 'X-Caller-Id: burst-demo' \
    http://127.0.0.1:18080/api/echo/b$i
done

# 非安全方法副作用（POST 只执行一次，不重试）
curl -s -X POST -H 'X-Caller-Id: premium' \
  http://127.0.0.1:18080/effects/side-effect
```

### 覆盖的关键场景

| 场景 | 用例 |
| --- | --- |
| 路由缺失明确拒绝 | `test_route_missing_is_rejected` |
| 全部上游不可用拒绝（可区分原因） | `test_all_upstreams_down_rejected_with_reason` |
| 单节点故障自动剔除、流量只走健康节点 | `test_failover_after_one_upstream_dies` |
| 恢复后自动重新纳入 | `test_upstream_recovery_rejoins` |
| 安全请求跨上游故障转移 | `test_get_retried_across_upstreams_on_failure` |
| 超时判定为上游失败（504，非 429） | `test_timeout_classified_as_upstream_failure` |
| 突发限流精确计数 | `test_rate_limit_burst_accurate` |
| 并发约束精确（10 并发恰好放行 3） | `test_concurrency_limit_parallel` |
| 缓存命中标记 + 键区分调用方/变体 | `test_cache_hit_marked_and_key_variants` |
| 缓存 TTL 失效 | `test_cache_expiry` |
| 缓存主动失效 | `test_cache_invalidation` |
| POST 不重试、无重复副作用 | `test_post_not_retried_no_duplicate_side_effect` |
| 终端 503 可解释 | `test_forced_503_failover_and_terminal_503` |
