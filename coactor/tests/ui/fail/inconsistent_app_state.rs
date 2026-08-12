use coactor::{ActorContext, ActorId, actor};

struct Inconsistent;

#[actor(name = "inconsistent")]
impl Inconsistent {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn first(&mut self, _context: &ActorContext<'_, String>) {}

    #[coactor::command]
    pub async fn second(&mut self, _context: &ActorContext<'_, Vec<u8>>) {}
}

fn main() {}

