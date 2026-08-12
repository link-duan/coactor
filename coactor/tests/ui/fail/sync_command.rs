use coactor::{ActorContext, ActorId, actor};

struct SyncCommand;

#[actor(name = "sync")]
impl SyncCommand {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}

