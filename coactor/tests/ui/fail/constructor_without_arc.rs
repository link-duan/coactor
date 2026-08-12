use coactor::{ActorId, CommandContext, actor};

struct InvalidActor;

#[actor(name = "constructor-without-arc")]
impl InvalidActor {
    pub fn new(_actor_id: ActorId, _state: ()) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
