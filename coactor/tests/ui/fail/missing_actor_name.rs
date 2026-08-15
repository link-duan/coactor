use coactor::{Actor, ActorRuntime, MessageContext, actor};

struct NoNameActor;

#[actor]
struct ActorWithoutName;

impl Actor<()> for ActorWithoutName {
    fn new(_runtime: ActorRuntime<()>) -> Self {
        Self
    }

    async fn on_message(&mut self, _ctx: &MessageContext, _msg: &[u8]) {}
}

fn main() {
    let _ = NoNameActor;
}
