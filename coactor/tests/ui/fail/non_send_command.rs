use std::rc::Rc;

use coactor::{ActorContext, ActorId, actor};

struct NonSend;

#[actor(name = "non-send")]
impl NonSend {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>, value: Rc<u8>) -> Rc<u8> {
        value
    }
}

fn main() {}

