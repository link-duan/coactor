# Use method-shaped typed Actor APIs in process

CoActor 首版对外提供 method-shaped typed API：每种 command 是 Actor Ref 上具有独立参数与返回类型的异步方法。Rust macro 负责生成内部消息封装、类型擦除、安全分发与 reply plumbing，consumer 不需要定义统一 command enum、reply enum 或手工匹配 reply variant。方法返回采用 Kameo 风格错误展平：handler 的 `Result<T, E>` 对 caller 表现为 `Result<T, SendError<E>>`，业务错误进入 `SendError::HandlerError(E)`；无业务错误的方法使用 `Infallible`，避免 runtime error 与业务 error 形成双层 `Result`。首版不照搬 Kameo 在部分 send error 中返还原始 message 的能力；`SendError` 不携带方法参数，caller 如需重试自行保留或重建参数。相比公开 `ActorAddress + bytes`，该方案为单进程 Rust library 提供更自然的调用体验，同时避免提前引入 wire format、schema 与解码错误。Actor Address 仍保持稳定且可独立表示，未来加入网络路由时可以在该边界上设计序列化协议，而不改变 Actor 的逻辑身份。

Actor Ref 是稳定地址句柄而非 Active Actor 强引用：获取、持有或 clone Ref 不会启动实例或阻止 passivation，调用 method 时才按 Actor Address 查找或懒启动。这样 consumer 可以长期缓存 Ref，而冷 Actor 的资源占用仍由 runtime lifecycle 决定。

Actor Ref 也只弱引用 CoActor runtime，不延长 runtime 生命周期；runtime 已停止或最后一个强 handle 释放后，已有 Ref 的调用返回 `RuntimeStopped`。runtime 的所有权和 shutdown 仍由 consumer 显式管理。

Actor Type 通过约定的同步 `new(actor_id)` 构造初始实例，不额外注册自定义 factory；需要异步执行的初始化统一放入 activation lifecycle，由 macro 在编译期连接这些约定。

只有显式标记 `#[command]` 的方法进入生成的 Actor Ref API；其他方法保持 Actor 内部实现，避免 public async helper 因可见性而意外成为消息协议的一部分。

被标记的方法必须是显式 `pub async fn`，generated Actor Ref method 同样 public；macro 不通过 attribute 隐式扩大 Rust 方法的可见性。

每个 Actor Type 生成专用 Ref 类型，例如 `RoomActorRef`，command 直接作为 inherent async methods 暴露。内部仍可包装通用 erased ref，但 consumer 不依赖 extension trait 导入或通用 message API。

macro 生成 Actor Type descriptor，但 runtime 只启用 consumer 在 builder 中显式 `register::<ActorType>()` 的类型；不使用自动扫描或 link-time inventory。这样 runtime 配置和重复名称检查在启动前可见且可测试。
