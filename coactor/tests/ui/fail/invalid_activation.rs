use std::sync::Arc;
use coactor::{ActorId, CommandContext, actor};

struct InvalidActivation;

#[actor(name = "invalid-activation")]
impl InvalidActivation {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    pub fn on_activate(&mut self) {}

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
