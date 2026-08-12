use coactor::{ActorContext, ActorId, DeactivationReason, actor};

struct InvalidDeactivation;

#[actor(name = "invalid-deactivation")]
impl InvalidDeactivation {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    pub async fn on_deactivate(
        &mut self,
        _context: &ActorContext<'_, ()>,
        _reason: DeactivationReason,
    ) -> Result<(), &'static str> {
        Ok(())
    }

    #[coactor::command]
    pub async fn call(&mut self, _context: &ActorContext<'_, ()>) {}
}

fn main() {}

