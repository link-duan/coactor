---
status: accepted
---

# Actor API

consumer 通过 macro 生成的 typed Actor Ref 使用 method-shaped request-response API；每个 command 保留独立参数、成功结果与业务错误类型，内部消息封装、类型擦除、dispatch 和 reply plumbing 对 consumer 隐藏。业务错误与 runtime failure 维持单层调用结果，runtime error 不携带原始业务参数；caller 如需安全重试，必须自行保留输入并根据错误类别决定。

每个 runtime 绑定一个 consumer 定义的强类型 App State，由全部 Actor Type 共享。依赖在 Actor construction 时注入；Command Context 只表示一次 invocation 的 Actor identity，不作为 service locator，也不提供隐式跨 Actor 调用。Actor Ref 只保存稳定 Actor Address 与 runtime 弱引用，不启动或保持 Active Actor 存活。

distributed runtime 延续同一个 typed API，而不向业务暴露 Node location。可跨节点调用的 command 使用显式 Protobuf request、response 与业务错误 payload，经版本化的通用 gRPC Invoke boundary 传输；首个 wire identity 使用 Rust method name，因此 method rename 与 Protobuf schema 兼容由 consumer 管理。API 继续只支持 request-response；caller timeout 只放弃 reply，不撤回已接纳 command。具体 Rust 类型和方法命名不在本 ADR 中冻结。

Actor Ref 是稳定地址句柄而非 Active Actor 强引用：获取、持有或 clone Ref 不会启动实例或阻止 passivation，调用 method 时才按 Actor Address 查找或懒启动。这样 consumer 可以长期缓存 Ref，而冷 Actor 的资源占用仍由 runtime lifecycle 决定。

Actor Ref 也只弱引用 CoActor runtime，不延长 runtime 生命周期；runtime 已停止或最后一个强 handle 释放后，已有 Ref 的调用返回 `RuntimeStopped`。runtime 的所有权和 shutdown 仍由 consumer 显式管理。

Actor Type 通过约定的同步 `new(actor_id, Arc<AppState>)` 构造初始实例，不额外注册自定义 factory；需要异步执行的初始化统一放入 activation lifecycle，由 macro 在编译期连接这些约定。

只有显式标记 `#[command]` 的方法进入生成的 Actor Ref API；其他方法保持 Actor 内部实现，避免 public async helper 因可见性而意外成为消息协议的一部分。

被标记的方法必须是显式 `pub async fn`，generated Actor Ref method 同样 public；macro 不通过 attribute 隐式扩大 Rust 方法的可见性。

每个 Actor Type 生成专用 Ref 类型，例如 `RoomActorRef`，command 直接作为 inherent async methods 暴露。内部仍可包装通用 erased ref，但 consumer 不依赖 extension trait 导入或通用 message API。

macro 生成 Actor Type descriptor，但 runtime 只启用 consumer 在 builder 中显式 `register::<ActorType>()` 的类型；不使用自动扫描或 link-time inventory。这样 runtime 配置和重复名称检查在启动前可见且可测试。
