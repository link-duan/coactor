use std::sync::Arc;

use coactor::{ActorId, CommandContext, actor};

struct InvalidActor;

#[actor(name = "constructor-parameter-order")]
impl InvalidActor {
    pub fn new(_state: Arc<()>, _actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
