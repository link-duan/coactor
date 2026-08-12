use coactor::{ActorContext, actor};

struct InvalidConstructor;

#[actor(name = "invalid-constructor")]
impl InvalidConstructor {
    pub async fn new() -> Self {
        Self
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}

