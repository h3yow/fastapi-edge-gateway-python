# FastAPI Edge Gateway（本地可运行）

一套可在本地完整运行的 API 边缘网关：用多个本地启动的 **上游模拟器实例** 模拟后端集群，
在统一入口下提供「动态路由 + 上游选择、调用方维度限流/并发约束、超时/安全重试/故障转移、
响应缓存」能力，并对每一类拒绝与命中给出**可区分、可解释**的结论。

## 1. 架构与模块

| 模块 | 职责 |
| --- | --- |
| `gateway/config.py` | 配置模型：上游、路由（前缀/策略/超时/重试/缓存 TTL）、调用方限流规则；**段边界前缀匹配** |
| `gateway/configfile.py` | 本地 JSON 配置文件的读取与**全量校验**（非法引用、非法额度等聚合报错） |
| `gateway/runtime.py` | 运行期配置容器：不可变版本快照 + 原子引用交换，版本私有缓存 |
| `gateway/router.py` | 健康感知的上游选择：轮询/加权随机、剔除、半开恢复探针（含错峰探针） |
| `gateway/health.py` | 每上游连续失败计数熔断；连续探针成功后自动重新纳入 |
| `gateway/ratelimit.py` | 按调用方的令牌桶限流 + 在途并发数约束（单锁，突发精确） |
| `gateway/cache.py` | 仅安全方法的响应缓存；键含调用方/路由/路径/查询/Vary 头；TTL 与失效 |
| `gateway/forwarder.py` | 转发：超时、**仅安全请求**重试、跨上游故障转移 |
| `gateway/app.py` | FastAPI 装配，统一的可区分错误码与决策响应头 |
| `upstream_sim/` | 本地后端模拟器（可冻结/恢复、可注入延迟与 5xx、副作用计数） |
| `tests/` | 单元测试 + 基于真实 HTTP 集群的端到端测试 |

## 2. 配置文件与运行期重载（运营可自助编排）

路由、上游清单、每条路由的分发策略/超时/重试/缓存、调用方维度的限流与并发额度，
都可以放进一份**本地可编辑的 JSON 配置**，启动时按配置生效，并可在运行期原子重载。

### 2.1 配置文件怎么写

参考仓库内的 `config.demo.json`；顶层结构：

| 字段 | 说明 |
| --- | --- |
| `version` | 必填、非空字符串。运营给本次配置的版本标识（如日期+序号），用于确认当前生效版本 |
| `upstreams[]` | 非空。`name` / `base_url` / `weight`(≥1) / `fail_threshold`(≥1) / `recover_threshold`(≥1) |
| `routes[]` | 非空。见下表 |
| `rate_limits[]` | 每个调用方：`caller` / `rate_per_sec`(≥1) / `burst`(≥1) / `max_concurrency`(≥1) |
| `defaults` | 未列名调用方的默认 `rate_per_sec` / `burst` / `max_concurrency`（均 ≥1） |

每条路由字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 路由唯一名（缓存键、日志、失效接口都用它） |
| `path_prefix` | 必须以 `/` 开头；按段边界匹配（见 §3） |
| `upstreams` | 非空上游名列表，**必须全部在 `upstreams` 中存在**，不可重复 |
| `strategy` | `round_robin`（默认）或 `random`（按 `weight` 加权） |
| `strip_prefix` | 转发前剥离的前缀；`null`/`""` 表示不剥离 |
| `cache_ttl_ms` | ≥0，0 表示不缓存 |
| `timeout_ms` | ≥1，毫秒 |
| `max_retries` | ≥0，仅安全请求实际重试 |

`base_url` 支持两种写法：普通 `http://` / `https://` 地址；或 `sim://u1` 这种形式，
在 `run_gateway.py` 本地拉起的模拟器集群里按节点名解析（测试与本地演示用）。

### 2.2 启动时按配置生效

```bash
# 带配置文件启动（本地演示：sim:// 节点会自动拉起）
uv run python run_gateway.py --port 18080 --config config.demo.json

# 不带 --config：仍使用 gateway/defaults.py 内置演示规则，行为与以前完全一致
uv run python run_gateway.py --port 18080
```

启动时若配置无法读取、JSON 非法或语义校验不过，进程**拒绝启动**（退出码 2），
逐条打印运维能看懂的原因，例如：

```
configuration invalid, aborting startup (/tmp/bad.json):
  - /tmp/bad.json: routes[0].upstreams[0]: references unknown upstream 'ghost' (known: ['u1', 'u2'])
  - /tmp/bad.json: rate_limits[0].burst: must be >= 1, got 0
```

### 2.3 运行期重新加载与查看版本（无需重启、不丢流量）

```bash
# 查看当前正在服务的版本及完整生效规则
curl -s http://127.0.0.1:18080/_gateway/config/version | python -m json.tool

# 编辑文件后触发重载（用启动时的 --config 路径；也可在 body 里 {"path": "..."} 覆盖）
$EDITOR config.demo.json
curl -s -X POST http://127.0.0.1:18080/_gateway/config/reload
```

重载语义（`gateway/runtime.py`）：

* **整份原子生效**：新文件先完整解析 + 全量校验，全部通过后才做一次引用交换；
  任何一步失败都**整份保持原样**，绝不出现一半新一半旧。
* **每请求同一版本快照**：请求进入时取一次当前不可变版本，其后路由、限流配额、
  缓存判定、上游选择与转发都基于这同一份；重载发生在请求处理途中也不会串版本。
  在途请求继续用旧版本跑完，新请求立即用新版本。
* **缓存按版本隔离**：新版本带一份全新缓存并清空旧版本缓存，
  旧规则下缓存的响应不可能在新规则生效后被返回（避免互相矛盾的结果）。
* 健康熔断状态与连接池跨重载保留（只换编排规则，不重置故障判定）。
* 每个响应都带 `X-Config-Version: <version>` 头，决策日志带
  `config_version=<version>`，可直接核对某请求用了哪一版规则。

重载失败时返回 `400`（JSON 语法/读盘问题同样按 400 处理），
body 给出全部原因和**仍在服务的旧版本**，旧配置继续服务：

```json
{
  "error": "invalid_config",
  "active_version": "demo-2026-10-08",
  "reasons": ["/...json: routes[0].upstreams[0]: references unknown upstream 'ghost' (known: ['u1', 'u2'])"]
}
```

用内置默认规则启动（未给 `--config`）时没有关联文件，重载返回
`409 {"error":"no_config_path",...}`；带 `--config` 重启即可获得重载能力。

## 3. 路由与上游选择规则

* 路由按 **最长路径前缀** 匹配（`config.route_for`），且匹配只在**路径段边界**上成立：
  `/v1/report` 命中 `/v1/report` 与 `/v1/report/...`，但**不会**吞掉只是前缀字符碰巧相同的
  `/v1/reporter`、`/v1/reportage`（这些按各自的规则处理，各自都没有则拒绝）。
  多条规则重叠时命中更长（更具体）的那条；未命中返回
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

## 4. 限流与并发约束

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

## 5. 超时、重试与故障转移

* 每条路由配置 `timeout_ms`；连接失败/网络错误/读超时分别映射为
  `504` + `X-Gateway-Reason: ConnectError / ReadTimeout / ...`。
* **只有安全可重放方法**（GET/HEAD/OPTIONS）允许重试；且每次重试必须切换到
  **不同的上游**（选择器通过 `exclude` 排除已尝试节点），不会在同一节点重放。
* 触发故障转移的条件：连接/网络错误、读超时、上游 502/503/504。
* 非安全方法**只发一次**：即便上游故障也不重试，从而保证
  「不产生重复副作用」（见 `test_post_not_retried_no_duplicate_side_effect`）。
* 所有候选都失败时，返回最后一次具体失败或统一的
  `503 upstream_unavailable`，并在响应头标注服务过的上游。

## 6. 缓存策略

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

## 7. 决策日志

所有关键判定以结构化日志输出到 stderr，单测运行时可见：

```
decision=route_lookup path=/api/echo/x route=echo verdict=match caller=premium config_version=demo-2026-10-08
decision=rate_limit caller=burst-demo verdict=reject reason=rate_limited tokens=0.000 in_flight=0
decision=upstream_select route=echo picked=u2 healthy=u2 strategy=round_robin reason=healthy_candidate
decision=failover route=echo failed=u1 reason=ConnectError next_attempt=2
decision=cache key=65328d8227a3 verdict=hit ttl_ms=2000
decision=upstream_removed upstream=u1 reason="consecutive_fails>=1"
decision=upstream_recovered upstream=u1 reason="probe_success>=1"
decision=config_reload verdict=applied version=demo-run-v2 revision=1 source=config.demo.json routes=6 upstreams=2 hash=47d8fc32c810
decision=config_reload verdict=reject source=config.demo.json errors=1
decision=config_reload_error source=config.demo.json detail="...: references unknown upstream 'ghost' ..."
```

## 8. 验证方法

```bash
# 依赖由 uv 管理（Python 3.14）
uv sync

# 全部测试：会真实启动两个本地上游实例（随机端口）
uv run pytest -q            # 35 个用例（原 18 + 配置/重载/段边界 17）
uv run pytest -s            # 同时观察上游选择 / 限流 / 缓存 / 配置版本决策日志

# 手动启动（内置默认规则，行为与历史一致）：网关 :8080（可 --port 覆盖）
uv run python run_gateway.py --port 18080

# 或按可编辑配置文件启动（支持运行期 reload；sim:// 自动绑定本地模拟器）
uv run python run_gateway.py --port 18080 --config config.demo.json

# 查看当前生效版本
curl -s http://127.0.0.1:18080/_gateway/config/version | python -m json.tool

# 段边界验证：/v1/report 命中 report；/v1/reporter 返回 route_not_found
curl -si -H 'X-Caller-Id: premium' http://127.0.0.1:18080/v1/report/echo/x | grep -iE 'x-upstream|x-gateway-reason'
curl -si -H 'X-Caller-Id: premium' http://127.0.0.1:18080/v1/reporter | grep -iE 'x-gateway-reason'

# 改完配置后原子重载；非法配置返回 400 与原因，旧版本继续服务
$EDITOR config.demo.json
curl -s -X POST http://127.0.0.1:18080/_gateway/config/reload | python -m json.tool

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
| 合法配置加载与版本元数据 | `tests/test_configfile_units.py` |
| 非法配置（不存在上游/非法额度/重名/坏 JSON）聚合拒绝 | `test_semantic_errors_are_aggregated` 等 |
| 段边界前缀不互相误命中 + 重叠取更具体 | `test_route_matching_segment_boundaries_and_specificity` / `test_adjacent_prefixes_do_not_swallow_each_other` |
| 运行期重载整份原子生效、版本可查 | `test_reload_applies_new_version_atomically` / `test_startup_version_is_reported` |
| 非法重载被拒绝且旧规则（含旧缓存）继续服务 | `test_invalid_reload_is_rejected_and_old_rules_keep_serving` |
| 重载与并发请求交叠时版本一致、不丢请求/不串规则 | `test_reload_concurrent_with_traffic_uses_consistent_versions` |
| 重载后配额立即生效、旧缓存不跨版本返回 | `test_quota_change_after_reload_is_enforced` / `test_reload_clears_old_cache_entries` |

### 与现有行为的兼容范围

* 不带 `--config` 启动时使用与以前完全相同的内置演示规则，调用方式、路由、
  限流、缓存、超时重试、故障转移行为均不变；原 18 个端到端/单元用例全部保留通过。
* 既有管理接口 `GET /_gateway/health`、`POST /_gateway/cache/invalidate` 语义不变。
* 新增响应头 `X-Config-Version`、新增管理接口 `GET /_gateway/config/version`、
  `POST /_gateway/config/reload`，不影响既有响应字段。
* 配置文件为可选能力：不提供文件不影响网关运行，仅运行期重载会得到 409 提示。
