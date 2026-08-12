use std::sync::Arc;
use coactor::{ActorId, CommandContext, actor};

struct PrivateCommand;

#[actor(name = "private")]
impl PrivateCommand {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command]
    async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
