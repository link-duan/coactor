use std::sync::Arc;
use coactor::{ActorId, CommandContext, DeactivationReason, actor};

struct InvalidDeactivation;

#[actor(name = "invalid-deactivation")]
impl InvalidDeactivation {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    pub async fn on_deactivate(
        &mut self,
        _reason: DeactivationReason,
    ) -> Result<(), &'static str> {
        Ok(())
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &CommandContext) {}
}

fn main() {}
