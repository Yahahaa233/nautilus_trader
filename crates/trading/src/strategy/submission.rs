//! 原生提交的两阶段约束接口，不负责数量计算、缓存登记或命令发送。
use nautilus_common::messages::execution::TradingCommand;

/// 缓存变更前检查命令并准备绑定所需状态；错误不改变原生订单缓存。
pub trait SubmissionInterceptor {
    fn prepare(&self, command: &TradingCommand) -> anyhow::Result<Box<dyn PreparedSubmission>>;
}

/// 原生缓存变更后、发送前完成绑定；错误阻止发送并产生对应拒绝事件。
pub trait PreparedSubmission {
    fn bind(self: Box<Self>, command: &TradingCommand) -> anyhow::Result<()>;
}
