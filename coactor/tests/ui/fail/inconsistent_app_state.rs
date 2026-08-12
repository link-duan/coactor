use coactor::{ActorId, CommandContext, actor};

struct Inconsistent;

#[actor(name = "inconsistent")]
impl Inconsistent {
    pub fn new(_actor_id: ActorId, _state: String) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn first(&mut self, _context: &CommandContext) {}
}

fn main() {}
