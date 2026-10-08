# FastAPI Edge Gateway（本地可运行）

一套可在本地完整运行的 API 边缘网关：用多个本地启动的 **上游模拟器实例** 模拟后端集群，
在统一入口下提供「动态路由 + 上游选择、调用方维度限流/并发约束、超时/安全重试/故障转移、
响应缓存」能力，并对每一类拒绝与命中给出**可区分、可解释**的结论。

## 1. 架构与模块

| 模块 | 职责 |
| --- | --- |
| `gateway/config.py` | 配置模型：上游、路由（前缀/策略/超时/重试/缓存 TTL）、调用方限流规则；**段边界感知**的最长前缀匹配 |
| `gateway/configfile.py` | 本地 JSON 配置文件的读取、解析与**整份校验**（收集全部可读错误原因） |
| `gateway/manager.py` | 运行期配置快照管理：按请求钉住版本、**原子切换**、可回滚的热加载 |
| `gateway/router.py` | 健康感知的上游选择：轮询/加权随机、剔除、半开恢复探针（含错峰探针） |
| `gateway/health.py` | 每上游连续失败计数熔断；连续探针成功后自动重新纳入 |
| `gateway/ratelimit.py` | 按调用方的令牌桶限流 + 在途并发数约束（单锁，突发精确） |
| `gateway/cache.py` | 仅安全方法的响应缓存；键含调用方/路由/路径/查询/Vary 头；TTL 与失效 |
| `gateway/forwarder.py` | 转发：超时、**仅安全请求**重试、跨上游故障转移 |
| `gateway/app.py` | FastAPI 装配，统一的可区分错误码与决策响应头 |
| `upstream_sim/` | 本地后端模拟器（可冻结/恢复、可注入延迟与 5xx、副作用计数） |
| `tests/` | 单元测试 + 基于真实 HTTP 集群的端到端测试 |

## 2. 路由与上游选择规则

* 路由按 **最长路径前缀** 匹配（`config.route_for`），且匹配是 **路径段边界感知** 的：
  只有「精确等于前缀」或「前缀之后紧跟 `/`」才算命中。因此配了 `/v1/report` 时：
  * `/v1/report`、`/v1/report/2026` 命中 `report` 路由；
  * `/v1/reporter`、`/v1/reporters/list` **不会**被吞进 `report`，而是按各自更宽的规则
    （如 `/v1`）处理；
  * 目录式前缀 `/api/` 仍正常匹配其子路径 `/api/echo`；前缀 `/` 匹配所有绝对路径。
* 多条规则重叠时，**更具体（更长）的前缀优先**（`/v1/report` 优先于 `/v1`）。
* 未命中任何路由时返回 `404 {"error":"route_not_found"}`，响应头
  `X-Gateway-Reason: route_not_found`，**绝不静默转发**到默认目标。
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

## 6. 运行期可配置与原子热加载

路由、上游清单、每条路由的分发策略/超时/重试/缓存、调用方维度的限流与并发额度，
都可以放进一份**本地可编辑的 JSON 文件**；网关启动时按文件生效，运行中可热加载，
无需发版、无需重启。

### 6.1 两种启动方式（原有方式保持不变）

```bash
# A. 原有演示方式：使用内置默认规则（行为与以前完全一致）
uv run python run_gateway.py --port 18080

# B. 配置文件方式：从本地 JSON 启动（--upstream-base-port 默认 19090，
#    内置模拟上游固定监听 19091/19092，方便在配置里写死地址）
uv run python run_gateway.py --config configs/demo.json --port 18080
```

`configs/demo.json` 与内置默认规则**逐条等价**，可直接复制后改成自己的编排规则。
启动时会对整份文件做校验；**任何不合法都会拒绝启动**（退出码 2），逐条打印运维可读
原因，且不会打开网关端口、不会产生半成品状态。

**兼容范围**：不传 `--config` 时，本地启动方式、内置默认演示规则、既有调用方式（路径、
请求头、错误码与错误体、`X-Cache/X-Upstream/X-Gateway-Reason` 等响应头语义）全部保持
不变；配置化能力是在其之上叠加的一层。程序化使用 `build_gateway(config)`（如测试夹具）
同样无需改动，首个代次即标记为内置默认规则。新增的 `X-Config-Revision/X-Config-Version`
响应头与 `/_gateway/config*` 管理端是纯增量，不影响既有字段。

### 6.2 配置文件怎么写

```json
{
  "version": "ops-2026-10-08-1",
  "defaults": {"rate_per_sec": 20, "burst": 20, "max_concurrency": 10},
  "upstreams": [
    {"name": "u1", "base_url": "http://127.0.0.1:19091",
     "weight": 1, "fail_threshold": 1, "recover_threshold": 1}
  ],
  "routes": [
    {"name": "report", "path_prefix": "/v1/report", "upstreams": ["u1"],
     "strategy": "round_robin", "strip_prefix": "/v1",
     "timeout_ms": 800, "max_retries": 2, "cache_ttl_ms": 2000}
  ],
  "rate_limits": [
    {"caller": "premium", "rate_per_sec": 100, "burst": 100,
     "max_concurrency": 20}
  ]
}
```

| 字段 | 说明 |
| --- | --- |
| `version` | 可选，运营自填的版本标签；响应头与 `/_gateway/config` 会回显 |
| `defaults` | 未显式配置的调用方使用的默认 `rate_per_sec / burst / max_concurrency`（均须正整数） |
| `upstreams[]` | `name`（唯一、非空）、`base_url`（必须 `http(s)://`）、`weight`、`fail_threshold`、`recover_threshold`（后三项可选正整数，默认 1） |
| `routes[]` | `name`（唯一）、`path_prefix`（唯一，必须以 `/` 开头，**按段边界匹配**）、`upstreams`（非空、不可重复、必须是已声明上游）、`strategy`（`round_robin`/`random`）、`strip_prefix`（字符串或 null）、`timeout_ms`（正整数）、`max_retries`、`cache_ttl_ms`（非负整数） |
| `rate_limits[]` | 每个 `caller` 一条（不可重复），三项配额都必填且为正整数 |

未列出的可选字段按数据类默认值补齐（路由默认 `round_robin`、`strip_prefix=null`、
`cache_ttl_ms=0`、`timeout_ms=1000`、`max_retries=0`）。出现未知字段、类型错误、
重复名字/前缀、引用不存在的上游、非法额度等，都会在加载/重载时被**整份拒绝**。

### 6.3 重新加载与查看当前版本

```bash
# 查看当前生效版本（revision 单调递增；version 为配置里的标签）
curl -s http://127.0.0.1:18080/_gateway/config | python -m json.tool

# 原子热加载：重新读取启动时指定的文件
curl -s -XPOST http://127.0.0.1:18080/_gateway/config/reload

# 也可以临时指定另一个文件路径
curl -s -XPOST http://127.0.0.1:18080/_gateway/config/reload \
  -H 'Content-Type: application/json' -d '{"path":"/tmp/new.json"}'
```

成功响应包含 `previous_revision/previous_version` 与新的 `revision/version`、
文件内容 `content_sha256`；内容字节完全相同则返回 `unchanged:true`，不产生新版本。

每个被代理的响应都带：

* `X-Config-Revision`：服务该请求的规则代次（整数）；
* `X-Config-Version`：该代次的运营标签。

`GET /_gateway/health` 也会附带当前 `config_revision/config_version`。

### 6.4 加载失败与回滚时的表现

* 重载先在**不影响线上**的前提下完成「读文件 → 解析 → 整份校验 → 装配新代次」，
  最后才用一次指针切换发布；**要么整份新生效，要么整份保持原样**，不存在一半新一半旧。
* 校验失败返回 `422`，头 `X-Gateway-Reason: config_reload_rejected`，响应体逐条给出
  运维可读原因（带 JSON 位置，如 `$.routes[0].upstreams: references unknown upstream
  'no-such'...`），并回显当前仍在服务的 `active_revision/active_version`。
* 失败时**旧配置继续提供完整服务**：路由、限流、缓存判定都还是旧代次。
* 启动期非法配置：拒绝启动，退出码 `2`，打印全部原因。

### 6.5 重载与并发流量交叠时的一致性保证

* 每个请求在入口**钉住一个不可变快照（snapshot）**：该请求的路由匹配、限流判定、
  缓存读写、上游选择全部基于同一代次，不会串规则或丢规则。
* 每一代次拥有**独立缓存**：重载后新请求从空缓存开始，旧缓存不可能绕过新规则返回
  互相矛盾的结果；旧请求若仍在执行，则继续用它钉住的旧代次直至结束。
* 限流/在途计数是全局共享的运行时状态（按 caller 维度），重载不重置也不重复计数：
  令牌预扣与并发占位仍在同一把锁内完成，请求结束精确释放，洪峰 + 反复调额度也不会
  多计、漏计或泄漏在途名额（见
  `test_concurrency_quota_reload_during_flood_never_leaks_or_overadmits`）。
* 上游健康状态按上游名跨代次保留，调路由/配额不会误把真实故障节点重新放回。

## 7. 决策日志

所有关键判定以结构化日志输出到 stderr，单测运行时可见；每条决策都带当前请求钉住的
`rev=`（代次）与 `ver=`（版本标签），可直接核对「这个请求用了哪一版规则、做了哪些
判定」：

```
decision=config_loaded revision=1 version=demo-built-in-equivalent-r1 sha=1de84cd5d67e routes=5 upstreams=2
decision=route_lookup rev=2 ver=ops-v2 path=/v1/report/x route=report verdict=match caller=premium
decision=rate_limit rev=2 ver=ops-v2 caller=burst-demo verdict=reject reason=rate_limited tokens=0.000 in_flight=0
decision=upstream_select rev=2 ver=ops-v2 route=report picked=u1 healthy=u1 strategy=round_robin reason=healthy_candidate
decision=failover route=echo failed=u1 reason=ConnectError next_attempt=2
decision=cache key=65328d8227a3 verdict=hit ttl_ms=2000
decision=config_reload old_revision=1 revision=2 old_version=r1 version=r2 sha=... verdict=applied
decision=config_reload_rejected old_revision=2 reason_count=5 reasons="$.routes[0]...."
decision=upstream_removed upstream=u1 reason="consecutive_fails>=1"
decision=upstream_recovered upstream=u1 reason="probe_success>=1"
```

## 8. 验证方法

```bash
# 依赖由 uv 管理（Python 3.14）
uv sync

# 全部测试：会真实启动两个本地上游实例（随机端口）
uv run pytest -q            # 45 个用例
uv run pytest -s            # 同时观察上游选择 / 限流 / 缓存 / 配置版本决策日志

# 手动启动（内置默认规则，行为不变）：网关 :8080（可 --port 覆盖），自动拉起 u1/u2
uv run python run_gateway.py --port 18080

# 手动启动（配置文件模式）：模拟上游固定在 19091/19092
uv run python run_gateway.py --config configs/demo.json --port 18080

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

### 配置热加载本地验证（配置文件模式下）

```bash
# 1) 查看当前版本
curl -s http://127.0.0.1:18080/_gateway/config | grep -E 'revision|version'

# 2) 编辑配置（改上游 / 配额 / 增删路由 / 调 TTL），然后原子热加载
$EDITOR configs/demo.json
curl -s -XPOST http://127.0.0.1:18080/_gateway/config/reload | python -m json.tool

# 3) 每个响应头都能看到服务它的是哪一版规则
curl -si http://127.0.0.1:18080/api/echo/x | grep -i x-config

# 4) 段边界：配 /v1/report 后，相邻资源不会被吞
curl -si http://127.0.0.1:18080/v1/report/1 | grep -i x-upstream     # report 路由
curl -si http://127.0.0.1:18080/v1/reporter/1 | grep -i x-upstream   # 按 /v1 规则

# 5) 故意写坏配置再 reload：返回 422 + 逐条原因，旧版本继续服务
curl -s -XPOST http://127.0.0.1:18080/_gateway/config/reload | python -m json.tool
curl -s http://127.0.0.1:18080/_gateway/config | grep revision       # 版本号不变
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
| 配置文件加载成功（版本/SHA/默认值） | `tests/test_configfile.py::test_load_valid_file_has_version_and_sha` |
| 非法 JSON / 缺文件 / 顶层非对象被拒 | `test_invalid_json_reports_position` 等 |
| 引用不存在上游、非法额度、重名重前缀被拒 | `test_route_referencing_unknown_upstream_rejected` 等 |
| 前缀相近路径不互相误命中 + 最长前缀优先 | `test_segment_boundary_matching`、`test_longest_specific_prefix_wins` |
| 热加载整份原子生效、旧路由整体下线 | `test_reload_success_switches_generation_atomically` |
| 非法重载被 422 拒绝且旧配置继续服务 | `test_invalid_reload_rejected_old_config_keeps_serving` |
| 相邻资源 `/echo/report` vs `/echo/reporter` 各走各的 | `test_sibling_prefixes_do_not_swallow_each_other` |
| 配额热改即时生效 | `test_quota_change_takes_effect_and_old_counters_keep_working` |
| 重载后旧缓存不串用、新代次冷启动 | `test_cache_isolated_across_reload_no_stale_hit` |
| 重载 × 并发流量交叠：判定自洽、代次一致 | `test_reload_interleaved_with_traffic_judgements_stay_consistent` |
| 洪峰 × 反复调并发额度：不超发、不漏放 | `test_concurrency_quota_reload_during_flood_never_leaks_or_overadmits` |
| 快照原子性 / 跨代缓存隔离 / 并发切换无撕裂 | `tests/test_manager.py` 全部 |
| 日志能看出请求用了哪一版规则 | `test_decision_logs_include_config_revision` |
