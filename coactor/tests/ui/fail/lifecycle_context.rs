use std::sync::Arc;

use coactor::{ActorId, CommandContext, actor};

struct InvalidActor;

#[actor(name = "lifecycle-context")]
impl InvalidActor {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    pub async fn on_activate(&mut self, _context: &CommandContext) -> Result<(), ()> {
        Ok(())
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
