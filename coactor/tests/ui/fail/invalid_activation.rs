use coactor::{ActorContext, ActorId, actor};

struct InvalidActivation;

#[actor(name = "invalid-activation")]
impl InvalidActivation {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    pub fn on_activate(&mut self, _context: &ActorContext<'_, ()>) {}

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}

