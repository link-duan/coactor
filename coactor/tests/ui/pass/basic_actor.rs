use coactor::{Actor, ActorRuntime, MessageContext, ServerBuilder, actor};

// `#[actor]` 无属性；Actor Type 名称由 consumer 在注册时显式传入。
#[actor]
struct CounterActor {
    value: i64,
}

impl Actor<()> for CounterActor {
    fn new(_runtime: ActorRuntime<()>) -> Self {
        Self { value: 0 }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        if let Ok(amount) = std::str::from_utf8(msg).unwrap().parse::<i64>() {
            self.value += amount;
            let _ = ctx.send(self.value.to_be_bytes().to_vec()).await;
        }
    }
}

fn main() {
    let _builder = ServerBuilder::local(()).register::<CounterActor>("counter");
}
