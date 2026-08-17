# Axum State 注入方式及其对 ServerBuilder 的启示

## 调研范围

本仓库当前依赖 Axum `0.7.9`。本次同时检查了 Axum `0.7.9` 与当前本地可用的 `0.8.9` 官方源码和 crate 文档；本文涉及的核心 API 形状在两个版本中一致。

主要来源：

- [Axum 0.8.9：Sharing state with handlers](https://docs.rs/axum/0.8.9/axum/#sharing-state-with-handlers)
- [Axum 0.8.9：Router::with_state](https://docs.rs/axum/0.8.9/axum/routing/struct.Router.html#method.with_state)
- [Axum 0.8.9：State extractor](https://docs.rs/axum/0.8.9/axum/extract/struct.State.html)
- [Axum 0.8.9：FromRef](https://docs.rs/axum-core/0.5.6/axum_core/extract/trait.FromRef.html)
- [Axum 官方源码：Router](https://github.com/tokio-rs/axum/blob/axum-v0.8.9/axum/src/routing/mod.rs)
- [Axum 官方源码：State](https://github.com/tokio-rs/axum/blob/axum-v0.8.9/axum/src/extract/state.rs)

## 结论摘要

Axum 不把 App State 放进 `Router::new(...)` 的构造参数，而是在路由组合完成后，通过 consuming builder 方法 `.with_state(state)` 注入：

```rust
let app = Router::new()
    .route("/", get(handler))
    .with_state(AppState { /* ... */ });
```

它的关键设计不是方法名称本身，而是以下几条：

1. State 是 Router 的全局强类型依赖，一个 Router 同一阶段只有一个顶层 State 类型。
2. `.with_state(state)` 消费 Router，并完成一次类型转换。
3. `Router<S>` 中的 `S` 表示 Router **仍缺少**的 State，而不是 Router 已经持有的 State。
4. 只有不再缺 State 的 `Router<()>` 能直接交给 server 运行。
5. Handler 通过 `State<T>` 获取顶层 State 或由 `FromRef` 派生出的 substate。
6. State 会在请求处理时 clone；官方建议大对象或共享可变状态使用 `Arc`，让 clone 足够便宜。

对 CoActor 来说，最值得借鉴的是“使用命名的 `.with_state(...)` 完成可选的强类型依赖注入，并让不调用该方法的 Server 使用 `()`”，而不是照搬 Axum 较反直觉的 `Router<S> = missing S` 泛型语义。

## Axum 的公开 API 形状

### 1. Router 先构建，State 后注入

标准使用方式是：

```rust
let app = Router::new()
    .route("/", get(handler))
    .with_state(state);
```

State 不属于 `Router::new` 的位置参数。这样 State 注入是一个可读的、具名的 builder 步骤，并且通常位于应用组合边界。Axum 官方还建议：可复用的路由函数返回一个仍缺 State 的 `Router<AppState>`，由最外层应用在真正启动前统一调用 `.with_state(...)`。

来源：[Router::with_state](https://docs.rs/axum/0.8.9/axum/routing/struct.Router.html#method.with_state)

### 2. `with_state` 是 consuming 类型转换

Axum `0.8.9` 的签名是：

```rust
pub fn with_state<S2>(self, state: S) -> Router<S2>
```

它消费当前 `Router<S>`，把当前缺少的 `S` 注入到已有 routes，并返回处于下一 State 阶段的 `Router<S2>`。内部会按需要 clone State，并把它装配到 path router、fallback router 和 endpoint。

来源：[Axum Router 官方源码](https://github.com/tokio-rs/axum/blob/axum-v0.8.9/axum/src/routing/mod.rs)、[Router::with_state](https://docs.rs/axum/0.8.9/axum/routing/struct.Router.html#method.with_state)

### 3. `Router<S>` 表示“缺少 S”

Axum 文档明确指出，`Router<AppState>` 的含义不是“持有 AppState”，而是“要处理请求还缺少 AppState”。调用：

```rust
let router: Router<()> = router.with_state(AppState {});
```

后，Router 才不再缺 State。只有这个阶段能直接传给 `axum::serve`，因此缺 State 会在编译期阻止启动。

这种模型主要服务于 Router 的 nest/merge 和 library composition。它很强，但也被官方文档称为有些反直觉，不宜在没有类似组合需求时机械复制。

来源：[Router::with_state — What `S` in `Router<S>` means](https://docs.rs/axum/0.8.9/axum/routing/struct.Router.html#what-s-in-routers-means)

### 4. State 是全局依赖，不是请求局部数据

Axum 把 State 定义为 Router 收到的所有请求都可使用的全局数据，典型内容包括配置、数据库连接池和其他服务 client。请求派生的数据（例如 middleware 解析出的认证信息）不应该放入 State。

这与 CoActor 当前定义的 App State 基本一致：它是同一 Server 内所有 Actor Type 共享的 consumer 强类型依赖容器。

来源：[State extractor](https://docs.rs/axum/0.8.9/axum/extract/struct.State.html)、[Sharing state with handlers](https://docs.rs/axum/0.8.9/axum/#sharing-state-with-handlers)

### 5. 顶层 State 与 substate

Axum 只有一个顶层 State 类型。Handler 如果只需要其中一部分，可以通过 `FromRef<OuterState>` 提取 substate：

```rust
impl FromRef<AppState> for ApiState {
    fn from_ref(state: &AppState) -> ApiState {
        state.api_state.clone()
    }
}
```

然后 Handler 使用：

```rust
async fn handler(State(api): State<ApiState>) {}
```

`State<InnerState>` 的 extractor 实现要求：

```rust
InnerState: FromRef<OuterState>
```

并从 `&OuterState` 产生一个 owned `InnerState`。Axum 还提供 `#[derive(FromRef)]` 简化这种投影。

来源：[State — Substates](https://docs.rs/axum/0.8.9/axum/extract/struct.State.html#substates)、[FromRef](https://docs.rs/axum-core/0.5.6/axum_core/extract/trait.FromRef.html)

### 6. Clone 与 Arc

Axum 的 State 会为请求处理 clone。官方推荐：

- 如果 State 的字段本身都是廉价 clone 的 handle，可以直接让 AppState 实现 `Clone`；
- 如果 State 较大，则注入 `Arc<AppState>`；
- 共享可变状态应使用合适的同步原语包裹。

来源：[Sharing state with handlers](https://docs.rs/axum/0.8.9/axum/#using-the-state-extractor)、[State — When states need to implement Clone](https://docs.rs/axum/0.8.9/axum/extract/struct.State.html#when-states-need-to-implement-clone)

## 与 CoActor 当前 State 语义的差异

CoActor 当前在构建 `Server` 时把 State 包装为 `Arc<S>`：

```rust
state: Arc::new(self.state)
```

每个 `ActorRuntime<S>` clone 的是这个 `Arc<S>`，并通过：

```rust
pub fn app_state(&self) -> &Arc<S>
```

暴露共享依赖。因此 CoActor 已经采用了 Axum 官方建议的大对象共享方式，而且不需要要求 consumer 的 `S: Clone`。

两者生命周期也不同：

- Axum 为每次 request 提取一个 State/substate 值；
- CoActor 在 Active Actor 构造时提供一次 `ActorRuntime<S>`，Actor 在整个 activation 生命周期内持有它。

因此可以借鉴 Axum 的注入形状，但不应该为了表面一致而引入 `S: Clone`、每 Action 提取 State 或完全复制 `FromRef` extractor 模型。

## 对新版 ServerBuilder 的建议

### 推荐调用形状

不需要业务 State：

```rust
let server = Server::builder(coordination_store)
    .node_id("node-a")
    .listen(bind_address)
    .advertise(advertised_address)
    .actor::<CounterActor>("counter")
    .start()
    .await?;
```

此时 Actor 使用 `ActorRuntime<()>`。

需要业务 State 时显式注入：

```rust
let server = Server::builder(coordination_store)
    .with_state(AppState)
    .node_id("node-a")
    .listen(bind_address)
    .advertise(advertised_address)
    .actor::<CounterActor>("counter")
    .start()
    .await?;
```

相比：

```rust
Server::builder(AppState, coordination_store)
```

这种形状有三个优势：

1. 不需要 State 的应用没有额外 ceremony，默认使用 `()`；
2. 需要 State 时，注入是具名步骤，不与 Coordination Store 形成两个含义不同的位置参数；
3. 与 Rust 生态中 Axum 的 `.with_state(...)` 习惯一致。

### 推荐类型语义

参考 Axum，让 builder 的泛型 `S` 表示其 Actor registrations 共同需要的 State 类型；未被其他调用约束时，`S` 默认推导为 `()`：

```rust
Server::builder(store)
    .actor::<UnitActor>("counter")
    .start();
```

需要业务 State 时，`.with_state(...)` 提供 `S` 的实际值。它既可以出现在 Actor registration 前，也可以出现在后面：

```rust
Server::builder(store)
    .with_state(AppState)
    .actor::<StatefulActor>("counter")
    .start();
```

```rust
Server::builder(store)
    .actor::<StatefulActor>("counter")
    .with_state(AppState)
    .start();
```

第二种形状中，`StatefulActor: ActorType<AppState>` 与 `.with_state(AppState)` 共同把 builder 的 `S` 推导为 `AppState`；registration 已经是 `Registration<AppState>`，因此不需要把 `Registration<()>` 转换成其他类型。

概念实现可以区分“仍缺 State 值”和“已有 State 值”两个阶段：

```rust
impl<S> Server<S> {
    pub fn builder<C>(store: C) -> ServerBuilder<C, S>;
}

impl<C, S> ServerBuilder<C, S> {
    pub fn actor<A>(self, name: ActorTypeName) -> Self
    where
        A: ActorType<S>;

    pub fn with_state(self, state: S) -> ReadyServerBuilder<C, S>;
}

impl<C> ServerBuilder<C, ()> {
    pub async fn start(self) -> Result<Server<()>, StartError>;
}

impl<C, S> ReadyServerBuilder<C, S> {
    pub fn actor<A>(self, name: ActorTypeName) -> Self
    where
        A: ActorType<S>;

    pub async fn start(self) -> Result<Server<S>, StartError>;
}
```

`ServerBuilder<C, ()>::start()` 隐式提供 unit State；其他 State 类型需要通过 `.with_state(...)` 进入 ready 阶段。公开类型名称不一定需要暴露 `ReadyServerBuilder`，也可以使用私有 typestate 参数表达。

### Builder 顺序

以下两种顺序都属于支持范围：

```rust
Server::builder(store)
    .with_state(state)
    .bind(...)
    .advertised_endpoint(...)
    .actor::<A>(...)
    .start()
```

```rust
Server::builder(store)
    .actor::<A>(...)
    .with_state(state)
    .bind(...)
    .advertised_endpoint(...)
    .start()
```

CoActor 只允许一个顶层 App State 类型，不复制 Axum 连续注入不同 State、继续组合新 routes 的多 State 行为。

## 建议结论

CoActor 应借鉴 Axum 的以下部分：

- 不调用 `.with_state(...)` 时默认使用 `()`；
- 需要业务 State 时使用 `.with_state(state)`，而不是把 State 作为 `builder(...)` 的位置参数；
- State 是 Server 范围内唯一的顶层强类型依赖；
- `.with_state(...)` 消费 builder，并切换其实际 State 类型；
- runtime 内部共享 State，consumer 不负责手动包装 `Arc`。

不建议照搬：

- `ServerBuilder<S>` 中 `S` 表示“仍缺少的 State”；
- State 必须实现 `Clone`；
- 每次 Action 都执行类似 extractor 的 State clone；
- 现阶段引入 `FromRef` substate 系统。

建议将新版入口定为：

```rust
// 无业务 State，默认 ()
Server::builder(coordination_store)

// 有业务 State，可以先注入
Server::builder(coordination_store)
    .with_state(AppState)

// 也可以先由 Actor registration 约束 State 类型
Server::builder(coordination_store)
    .actor::<StatefulActor>("counter")
    .with_state(AppState)
```

`.with_state(...)` 提供 builder 已推导出的唯一顶层 State 值，而不是要求它必须发生在 Actor registration 之前。
