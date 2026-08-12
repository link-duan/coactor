use std::sync::Arc;
use coactor::{ActorId, CommandContext, actor};

struct NotDebug;
struct NonDebugError;

#[actor(name = "non-debug-error")]
impl NotDebug {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext) -> Result<(), NonDebugError> {
        Err(NonDebugError)
    }
}

fn main() {}
