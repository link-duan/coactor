use coactor::actor;

// `#[actor]` 不接受任何属性：Actor Type 名称由 consumer 在注册时显式传入。
#[actor(name = "counter")]
struct CounterActor;

fn main() {}
