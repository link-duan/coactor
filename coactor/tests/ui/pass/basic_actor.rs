use std::sync::Arc;

use coactor::{ActorId, CommandContext, RuntimeBuilder, actor};

struct Counter(i64);

#[actor(name = "counter")]
impl Counter {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self(0)
    }

    pub async fn on_activate(
        &mut self,
    ) -> Result<(), &'static str> {
        Ok(())
    }

    pub async fn on_deactivate(
        &mut self,
        _reason: coactor::DeactivationReason,
    ) {
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &CommandContext, amount: i64) -> i64 {
        self.0 += amount;
        self.0
    }

    #[coactor::command]
    pub async fn checked(&mut self, _context: &CommandContext) -> Result<i64, &'static str> {
        Ok(self.0)
    }
}

fn main() {
    let _runtime = RuntimeBuilder::new(())
        .register::<Counter>()
        .build()
        .unwrap();
}
