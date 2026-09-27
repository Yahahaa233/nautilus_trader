> Current fork base: NautilusTrader v2.0.0rc5, upstream commit 1b0a49d2792a9432a3aca3fcb617ce7a630d905e.
> Imported from Metis vendor manifest; earlier versions below are historical provenance.

> 历史记录：下文保留旧版本的来源与验证结论。当前 rc5 基线（2026-09-28 升级）、差异和验证入口见 [受管组件说明](../README.md)。

> rc5 升级注记（2026-09-28）：上游已吸收并退役的补丁——订单列表整组预验证（rc5 `validate_order(order, trade_mode, OrderSubmission::List)` 原生同语义）、`OKXAccountConfiguration` 类型化（metis 简单版退役，仅保留 `uid` 字段补丁）、`get_account_configuration`、`OKXAlgoOrderType::SmartIceberg`/`Chase`。rc5 变更适配：`paginate_algo_pending` 增加 404 覆盖门控参数，metis `collect_pending_pages` 严格语义保留于函数体；凭证/代理改 `SecretString.expose_secret()` 模式。验证：根工作区 `cargo check -p nautilus-okx` 零警告；vendor 工作区 867 项测试通过。


# OKX 适配器来源

上游：nautechsystems/nautilus_trader，v1.231.0，提交 27a8e54e7ac3c57d6cbf8891f0283dfbaee97317。

本目录保存该版本的 OKX 包源码。清单改为独立工作区，订单模型统一使用同版本的 `../nautilus-model` 受管源码，其余 Nautilus 包仍使用同一官方版本。CQS 改动范围为改单请求与回报关联、只读账户配置查询；不新增交易所客户端。根工作区通过 Cargo 源覆盖统一选择本目录，依赖树已确认，不与另一份 OKX 实现同时运行。

当前改动：单笔 Rust 改单使用原命令 UUID 的 32 位表示作为请求 ID；同一订单有待确认修改时禁止覆盖登记。请求接收成功和不明确的发送失败保留登记，订单频道按 reqId 确认结果；修改结果与同时成交/取消分别派发。拒绝回报只有匹配当前请求才能关闭登记，更新与拒绝事件携带 causation_id。

本地参数拒绝、明确交易所拒绝及订单频道失败共用拒绝事件构造，均携带原命令标识；频道失败先关闭匹配登记再派发事件。

仍待完成：完整异常检查、保护单改量和生产装配。独立限价入场改单的预算及回报已接入 CQS 统一提交状态机，本地证据见系统引擎实施计划。Python 和批量改单路径没有完成相同合同验证，不属于本次可启用范围。

验证：CQS 非默认功能节点使用根清单完成 `cargo check --offline -p cqs-node --no-default-features`，依赖树检查使用 `--locked --offline`；新增改单派发测试包含请求接收、旧请求拒绝、断连、订单频道成功/失败/自动取消、同时成交及重复推送，本地验证拒绝测试断言原 causation_id。适配器库测试 640 项、执行客户端 57 项、WebSocket 36 项全部通过，共 733 项。本地模拟服务器证据不代表交易所验收或仓位引擎生产装配完成。

订单列表补充修复：条件单与非 GTC 和非法客户端 ID 一起完成整组预验证。失败时所有订单收到 Denied，不再直接抛错后缺少终态；执行客户端专项 59 项通过。原生条件单改单的 HTTP 执行路由与回报关联仍待实现，普通 WebSocket 修改不支持触发价字段，不能把 CQS 本地保护改单测试作为该路径验收。

条件单改单回报补充（2026-09-09）：HTTP 请求增加可选 reqId；WS 条件单消息接收 reqId/amendResult，dispatcher 复用 pending_amends 产生关联确认和拒绝，拒绝旧请求及重复 live 状态改写。库测试 641 项通过；HTTP 修改执行路由仍未接入，trigger 类修改参数需依据官方合同核实。

条件单 HTTP 修改执行（2026-09-09）：未触发订单复用原生订单路由与 pending_amends，查询核对真实身份和类型后发送，conditional 修改对应止盈/止损字段，trigger 仅发送已核实改量。未知结果不重发、不完成；HTTP amend-algos 禁用自动重试。库 645、执行端 68、HTTP 98 项测试通过。计划委托改价字段仍无已核实合同，明确拒绝；不表示 ADX 保护改单、父子联动或生产验收完成。

只读账户配置（2026-09-09）：原 RawHttpClient 新增 `GET /api/v5/account/config`，复用认证、解码和错误处理；模型要求 uid、acctLv、posMode、autoLoan，不为缺失模式设置默认值。节点查询模式后查询余额，共用完整请求时间范围。不修改交易所账户设置，也不将两次请求视为原子快照。
账户配置验证：节点观测 9 项、账户配置 HTTP 专项 1 项、Guardian 回归 20 项通过。未执行真实交易所查询，完整 HTTP 套件未在本轮重跑。

挂单分页完整性（2026-09-09）：普通和条件挂单共用分页收集器。无显式结果上限时，必须取得末页；页数耗尽、重复标识、空标识、超页响应、中途请求失败均返回错误。条件挂单 HTTP 404 不再作为空列表。保留显式结果数量限制，不将截取请求当作完整账户查询；历史订单、历史条件单和价差订单分页尚未完成同等重构。
挂单分页验证：库 649 项、HTTP 100 项全部通过，日志 `/tmp/cqs-pending-pagination.log`。

订单有效期转换（2026-09-09）：普通及价差报告的重复转换抽取为 `common::parse::parse_time_in_force`，节点挂单采集与已有转换测试共用，未改变 FOK/IOC/GTC 映射。库 649 项、HTTP 100 项通过，日志 `/tmp/cqs-order-tif-reuse.log`。

算法挂单查询（2026-09-09）：增加 Chase、SmartIceberg 的枚举识别，账户查询按 QUERY_TYPES 八类读取原始完整分页；Any 省略 instType，任一失败、类型错配或跨类型重复不返回部分成功。库 649、HTTP 101 项通过，日志 `/tmp/cqs-all-algo-query.log`。未开放新交易算法，节点接入仍待完成。

算法观测接入（2026-09-09）：上述查询已进入节点原账户观测。新增 OKXTriggerType 到 TriggerType 的唯一转换，节点采集与原 WebSocket 解析共用；没有改变发送端默认触发类型行为。库 649、HTTP 101 项回归通过，日志 `/tmp/cqs-algo-trigger-conversion.log`。CQS 固定保护单对应核对不代表适配器新增算法执行或生产保证金验收。

历史预热交付（2026-09-13）：DataClient 在一次响应交付前对错误、空结果和短结果最多请求三次；各次中间结果不喂指标。历史解析忽略 confirm != 1 的未完成 K 线。策略预热使用向过去分页，收到最终不足结果仍保持阻塞。代码验证与运行验收分别记录于 docs/plans/2026-09-13-demo-warmup-repair.md；未部署交易节点。
