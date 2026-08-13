use std::sync::Arc;

use coactor::{ActorId, CommandContext, actor};

struct MultipleRequests;

#[actor(name = "multiple-requests")]
impl MultipleRequests {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    #[coactor::command(remote)]
    pub async fn call(
        &mut self,
        _context: &CommandContext,
        first: FirstRequest,
        second: SecondRequest,
    ) {
    }
}

struct FirstRequest;
struct SecondRequest;

fn main() {}
