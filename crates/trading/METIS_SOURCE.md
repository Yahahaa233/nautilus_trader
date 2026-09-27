> Current fork base: NautilusTrader v2.0.0rc5, upstream commit 1b0a49d2792a9432a3aca3fcb617ce7a630d905e.
> Imported from Metis vendor manifest; earlier versions below are historical provenance.

> 历史记录：下文保留旧版本的来源与验证结论。当前 rc5 基线（2026-09-28 升级）、差异和验证入口见 [受管组件说明](../README.md)。

# 原生策略提交接口来源

上游：nautechsystems/nautilus_trader，v1.231.0，提交 27a8e54e7ac3c57d6cbf8891f0283dfbaee97317。

本目录保存同版本 trading 包，清单转换为独立工作区；根工作区统一选择此包，其他依赖沿用固定版本。新增原生单单及订单列表的两阶段提交接口：缓存登记前准备、登记后绑定，成功后沿原初始化发布、原队列及到期处理执行。任一阶段错误禁止发送；登记后绑定错误沿原拒单方法终结订单。

未安装接口的调用沿用原行为；该默认行为不构成 CQS 生产启用授权。节点必须另行验证注入与仓位强制检查。Python/扩展模块和真实交易所尚未验收。

原生单笔改单使用同一两阶段接口：生成最终命令后、PendingUpdate 前准备，缓存变更后绑定。绑定失败发布携带原 command_id 的 OrderModifyRejected，保留原委托数量和价格；成功沿原命令路由发送。安装接口时批量改单在缓存变更前明确拒绝，尚未实现批量原子绑定。
