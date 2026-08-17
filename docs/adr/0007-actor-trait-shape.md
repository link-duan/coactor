---
status: accepted
---

> 注：Actor Type 名称移出 macro 的决策仍然有效；具体注册 API 与 client 侧 `ActorRef` 寻址形态已由 [ADR-0011](0011-chainable-runtime-api.md) supersede。本 ADR 正文保留命名 macro 与早期 Ref API 的历史背景，当前仍有效的核心决策是 struct 挂 macro、consumer 手写 `Actor` trait 与原生 `async fn`。

# Actor trait 形态：struct 挂 macro + 原生 async fn + 按名字索引

`#[coactor::actor(name = "...")]` 只挂在 Actor struct 上，生成 `ACTOR_NAME` 常量与注册实现；consumer 手写 `impl Actor<S>`，业务逻辑与生命周期 hook 全部在 trait 中表达。macro 不再扫描 impl block 或方法签名，职责收缩为样板生成与类型擦除的 dispatch 适配（`ActorType::handle`/`session_opened`/`session_closed`）。

`Actor` trait 的方法使用**原生 `async fn`**，依赖 `trait Actor<S>: Send` 的 supertrait 让 future 自动 `Send`（当前工具链验证可行）；早期尝试显式 `BoxFuture<'a>` 返回类型与 `#[async_trait]`，前者使 consumer 侧样板过多，后者引入生命周期不匹配，均被放弃。`#[allow(async_fn_in_trait)]` 关闭"public trait async fn 无法显式指定 auto trait bounds"的 lint——我们依赖 Send 自动继承，已足够。

client 侧不生成 per-Actor 的专用 Ref 类型，改为按 Actor Type 名字索引：`runtime.actor("room", actor_id)` 返回通用 `ActorRef`（无类型参数），`open()` 建立 `Session`。由于消息是字节、`open` 签名与类型无关，专用类型不再携带信息；未注册名字在获取时立即报错。`Session`/`SessionHandle` 因此也是非泛型类型。
