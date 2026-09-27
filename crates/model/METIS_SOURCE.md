> Current fork base: NautilusTrader v2.0.0rc5, upstream commit 1b0a49d2792a9432a3aca3fcb617ce7a630d905e.
> Imported from Metis vendor manifest; earlier versions below are historical provenance.

> 历史记录：下文保留旧版本的来源与验证结论。当前 rc5 基线（2026-09-28 升级）、差异和验证入口见 [受管组件说明](../README.md)。

> rc5 升级注记（2026-09-28）：ONEINCH 注册已由上游 `currency_constants!` 宏吸收，metis 注册补丁退役；交错拒绝守卫保持 `(ModifyRejected, PendingUpdate) | (CancelRejected, PendingCancel)`，rc5 冗余臂已去重；denied_reason 文档表按 rc5 枚举重新生成（regeneration 测试通过）。


# 订单模型来源

上游：nautechsystems/nautilus_trader，v1.231.0，提交 27a8e54e7ac3c57d6cbf8891f0283dfbaee97317。

本目录保存该版本的模型包，清单改为独立工作区。根工作区和受管 OKX 的独立测试工作区统一使用本目录；不同时运行另一份模型实现。

构建脚本同步跟踪本目录的 `Cargo.toml`。原 `../Cargo.toml` 在受管目录结构中不存在，导致 Cargo 每次重新运行构建脚本并重建依赖链；本项只修正构建输入路径，不改变模型行为。

修复范围：真实部分成交、取消或终结可先于修改拒绝到达。`OrderCore` 仅在当前状态仍为 `PendingUpdate` 时恢复修改前状态；其他允许状态保留最新状态，并记录拒绝事件。修改拒绝不撤销成交、不改变订单数量、不恢复已终结订单，也不创建虚假的待修改事件。原修改成功及成交处理不变。

该源码调整来自 CQS 的真实缓存回归失败：`PendingUpdate → Filled（部分成交）→ ModifyRejected` 在原模型的最后一步返回 `Invalid order state transition`。验证结果记录在 CQS 实施计划；源码存在不等于运行验收通过。Python、FFI 和其他未执行功能组合不包含在当前验证结论中。
