use coactor::{ActorContext, ActorId, actor};

struct PrivateCommand;

#[actor(name = "private")]
impl PrivateCommand {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    async fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}

