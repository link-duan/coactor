use coactor::{ActorContext, ActorId, actor};

struct NotDebug;
struct NonDebugError;

#[actor(name = "non-debug-error")]
impl NotDebug {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>) -> Result<(), NonDebugError> {
        Err(NonDebugError)
    }
}

fn main() {}

