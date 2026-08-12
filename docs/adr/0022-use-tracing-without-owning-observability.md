# Use tracing without owning observability infrastructure

CoActor 首版使用 Rust `tracing` 产出结构化 spans/events，用于诊断 activation error、handler panic、deactivation timeout 和 runtime shutdown，但 library 不安装全局 subscriber。事件包含 Actor Type、Actor ID、lifecycle 阶段和错误类别，不记录 command 参数或返回值，以避免默认泄露业务数据。metrics registry、管理端口和自定义 observer trait 暂不提供，由宿主应用决定日志采集与输出方式。
