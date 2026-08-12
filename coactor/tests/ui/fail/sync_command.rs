use std::sync::Arc;
use coactor::{ActorId, CommandContext, actor};

struct SyncCommand;

#[actor(name = "sync")]
impl SyncCommand {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command]
    pub fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
