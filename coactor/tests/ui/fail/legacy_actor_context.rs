use std::sync::Arc;

use coactor::{ActorContext, ActorId, actor};

struct InvalidActor;

#[actor(name = "legacy-actor-context")]
impl InvalidActor {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}
