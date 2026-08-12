# Use the consumer's Tokio runtime

CoActor 首版作为 Rust library 运行在 consumer 已有的 Tokio runtime 中，只 spawn 自身 Actor tasks，不创建隐藏线程、专用 executor 或嵌套 runtime。在线服务通常已经由 Axum、游戏 server 或其他 Tokio 组件管理线程和进程生命周期；library 自建 runtime 会造成调度、资源限制和 shutdown 冲突。consumer 负责显式启动并 await CoActor graceful shutdown。

显式 await graceful shutdown 是 consumer 的使用契约。直接 drop 最后一个 runtime handle 属于误用，CoActor 不承诺 mailbox drain、deactivation 或 pending reply 结果；首版可以采用直接取消、诊断或 panic 等简单处理，而不把这种路径塑造成另一套 shutdown protocol。

为支持 Tokio 多线程调度，首版沿用 Kameo 的边界：Actor、command 参数、reply 和 handler future 为 `Send + 'static`，业务错误额外要求 `Debug`；Actor 不要求 `Sync`。`!Send` Actor 与 `LocalSet` 模式不在首版范围内。
