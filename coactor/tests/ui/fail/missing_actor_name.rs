use coactor::{ActorContext, ActorId, actor};

struct MissingName;

#[actor]
impl MissingName {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}

