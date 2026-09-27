> 历史记录：下文保留旧版本的来源与验证结论。当前 rc5 基线（2026-09-28 升级）、差异和验证入口见 [受管组件说明](../README.md)。

Upstream: https://github.com/nautechsystems/nautilus_trader.git tag v1.231.0, checkout 27a8e54e7ac3c57d6cbf8891f0283dfbaee97317.
Local patch: read-only engine-issued liquidation provenance, scoped to matching-engine lifecycle. No strategy-created proof, no fill or liquidation arithmetic changes.
