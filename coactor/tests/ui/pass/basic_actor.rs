use coactor::{actor, Actor, ActorRuntime, MessageContext, test_support::TestServer};

#[actor]
struct CounterActor;

impl Actor<()> for CounterActor {
    fn new(_: ActorRuntime<()>) -> Self { Self }
    async fn on_message(&mut self, _ctx: &MessageContext, _msg: &[u8]) {}
}

fn main() {
    let _builder = TestServer::builder().actor::<CounterActor>("counter");
}
