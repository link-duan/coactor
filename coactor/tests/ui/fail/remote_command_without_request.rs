use std::sync::Arc;

use coactor::{ActorId, CommandContext, actor};

struct MissingRequest;

#[actor(name = "missing-request")]
impl MissingRequest {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command(remote)]
    pub async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
