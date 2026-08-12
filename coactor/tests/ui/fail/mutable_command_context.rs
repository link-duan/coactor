use std::sync::Arc;

use coactor::{ActorId, CommandContext, actor};

struct InvalidActor;

#[actor(name = "mutable-command-context")]
impl InvalidActor {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &mut CommandContext) {}
}

fn main() {}
