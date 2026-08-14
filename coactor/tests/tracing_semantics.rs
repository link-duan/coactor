use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use coactor::{ActorId, CommandContext, RuntimeBuilder, SendError, actor};
use tokio::sync::Notify;
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
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    pub async fn on_activate(&mut self) -> Result<(), &'static str> {
        Err("private-activation-detail")
    }

    #[coactor::command]
    pub async fn secret_command(&mut self, _context: &CommandContext, _secret: &'static str) {}
}

struct LifecycleActor;

#[actor(name = "traced-lifecycle")]
impl LifecycleActor {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self
    }

    pub async fn on_activate(&mut self) -> Result<(), &'static str> {
        Ok(())
    }

    pub async fn on_deactivate(&mut self, _reason: coactor::DeactivationReason) {}

    #[coactor::command]
    pub async fn ping(&mut self, _context: &CommandContext) {}
}

#[derive(Clone)]
struct FailureState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct PanicActor;

#[actor(name = "traced-panic")]
impl PanicActor {
    pub fn new(_actor_id: ActorId, _state: Arc<FailureState>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn panic_now(&mut self, _context: &CommandContext) {
        panic!("handler panic")
    }
}

struct DeactivationTimeoutActor(Arc<FailureState>);

#[actor(name = "traced-deactivation-timeout")]
impl DeactivationTimeoutActor {
    pub fn new(_actor_id: ActorId, state: Arc<FailureState>) -> Self {
        Self(state)
    }

    pub async fn on_deactivate(&mut self, _reason: coactor::DeactivationReason) {
        self.0.entered.notify_one();
        self.0.release.notified().await;
    }

    #[coactor::command]
    pub async fn ping(&mut self, _context: &CommandContext) {}
}

struct ShutdownTimeoutActor(Arc<FailureState>);

#[actor(name = "traced-shutdown-timeout")]
impl ShutdownTimeoutActor {
    pub fn new(_actor_id: ActorId, state: Arc<FailureState>) -> Self {
        Self(state)
    }

    #[coactor::command]
    pub async fn block(&mut self, _context: &CommandContext) {
        self.0.entered.notify_one();
        self.0.release.notified().await;
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
    let runtime = RuntimeBuilder::local(())
        .register::<FailingActor>()
        .start()
        .await
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

#[tokio::test]
async fn successful_activation_and_shutdown_deactivation_are_traced() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(captured.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let runtime = RuntimeBuilder::local(())
        .register::<LifecycleActor>()
        .start()
        .await
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<LifecycleActor>(ActorId::from("lifecycle-1"))
        .expect("registered Actor Type");

    actor.ping().await.unwrap();
    runtime.shutdown().await;

    let output = captured.text();
    assert!(output.contains("actor_type=\"traced-lifecycle\""));
    assert!(output.contains("lifecycle=\"activation\""));
    assert!(output.contains("lifecycle=\"deactivation\""));
    assert!(output.contains("reason=\"Shutdown\""));
    assert!(output.contains("error_category=\"None\""));
}

#[tokio::test(start_paused = true)]
async fn important_failure_events_have_structured_tracing_fields() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(captured.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let panic_runtime = RuntimeBuilder::local(FailureState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    })
    .register::<PanicActor>()
    .start()
    .await
    .unwrap();
    let panic_actor = panic_runtime
        .actor_ref::<PanicActor>(ActorId::from("panic-1"))
        .unwrap();
    assert_eq!(panic_actor.panic_now().await, Err(SendError::ActorStopped));
    panic_runtime.shutdown().await;

    let idle_state = FailureState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let idle_runtime = RuntimeBuilder::local(idle_state.clone())
        .idle_timeout(Duration::from_secs(1))
        .deactivation_timeout(Duration::from_secs(1))
        .register::<DeactivationTimeoutActor>()
        .start()
        .await
        .unwrap();
    let idle_actor = idle_runtime
        .actor_ref::<DeactivationTimeoutActor>(ActorId::from("idle-timeout-1"))
        .unwrap();
    idle_actor.ping().await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    idle_state.entered.notified().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    idle_runtime.shutdown().await;

    let shutdown_state = FailureState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let shutdown_runtime = RuntimeBuilder::local(shutdown_state.clone())
        .shutdown_timeout(Duration::from_secs(1))
        .register::<ShutdownTimeoutActor>()
        .start()
        .await
        .unwrap();
    let shutdown_actor = shutdown_runtime
        .actor_ref::<ShutdownTimeoutActor>(ActorId::from("shutdown-timeout-1"))
        .unwrap();
    let blocked = tokio::spawn(async move { shutdown_actor.block().await });
    shutdown_state.entered.notified().await;
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    shutdown.await.unwrap();
    assert_eq!(blocked.await.unwrap(), Err(SendError::ActorStopped));

    let output = captured.text();
    assert!(output.contains("actor_type=\"traced-panic\""));
    assert!(output.contains("actor_id=ActorId"));
    assert!(output.contains("lifecycle=\"command\""));
    assert!(output.contains("error_category=\"ActorStopped\""));
    assert!(output.contains("actor_type=\"traced-deactivation-timeout\""));
    assert!(output.contains("lifecycle=\"deactivation\""));
    assert!(output.contains("error_category=\"DeactivationTimedOut\""));
    assert!(output.contains("lifecycle=\"shutdown\""));
    assert!(output.contains("error_category=\"ShutdownTimedOut\""));
}
