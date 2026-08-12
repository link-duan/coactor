use std::{
    io,
    sync::{Arc, Mutex},
};

use coactor::{ActorContext, ActorId, RuntimeBuilder, SendError, actor};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.0.clone())
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

struct FailingActor;

#[actor(name = "traced-failure")]
impl FailingActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    pub async fn on_activate(
        &mut self,
        _context: &ActorContext<'_, ()>,
    ) -> Result<(), &'static str> {
        Err("private-activation-detail")
    }

    #[coactor::command]
    pub async fn secret_command(&mut self, _context: &ActorContext<'_, ()>, _secret: &'static str) {
    }
}

#[tokio::test]
async fn lifecycle_failures_emit_identity_and_category_without_command_payloads() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(captured.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let runtime = RuntimeBuilder::new(())
        .register::<FailingActor>()
        .build()
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<FailingActor>(ActorId::from("actor-42"))
        .expect("registered Actor Type");

    assert_eq!(
        actor.secret_command("do-not-log-this").await,
        Err(SendError::ActivationFailed)
    );

    let output = captured.text();
    assert!(output.contains("actor_type=\"traced-failure\""));
    assert!(output.contains("actor_id=ActorId"));
    assert!(output.contains("lifecycle=\"activation\""));
    assert!(output.contains("error_category=\"ActivationFailed\""));
    assert!(output.contains("private-activation-detail"));
    assert!(!output.contains("do-not-log-this"));
}
