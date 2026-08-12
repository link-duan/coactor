use std::rc::Rc;

use coactor::{ActorId, CommandContext, actor};

struct NonSend;

#[actor(name = "non-send")]
impl NonSend {
    pub fn new(_actor_id: ActorId, _state: std::sync::Arc<()>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext, value: Rc<u8>) -> Rc<u8> {
        value
    }
}

fn main() {}
