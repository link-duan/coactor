use std::{collections::HashMap, io::IsTerminal};

use coactor::{Actor, ActorRuntime, MessageContext, Server, SessionId, actor};

#[path = "chat/protocol.rs"]
mod protocol;
#[path = "chat/storage.rs"]
mod storage;

use protocol::{ChatAction, ChatEvent, encode};
use storage::coordination_store;

#[actor]
struct ChatRoom {
    runtime: ActorRuntime<()>,
    members: HashMap<SessionId, String>,
}

impl ChatRoom {
    async fn broadcast(&self, event: &ChatEvent) {
        self.runtime.broadcast(encode(event)).await;
    }

    async fn reject(ctx: &MessageContext, message: impl Into<String>) {
        let message = message.into();
        tracing::warn!(actor = %ctx.actor_address(), %message, "chat Action rejected");
        let _ = ctx.send(encode(&ChatEvent::Error { message })).await;
    }
}

impl Actor<()> for ChatRoom {
    fn new(runtime: ActorRuntime<()>) -> Self {
        Self {
            runtime,
            members: HashMap::new(),
        }
    }

    async fn on_message(&mut self, ctx: &MessageContext, message: &[u8]) {
        let action = match serde_json::from_slice::<ChatAction>(message) {
            Ok(action) => action,
            Err(_) => {
                Self::reject(ctx, "invalid chat Action").await;
                return;
            }
        };
        let session_id = ctx.session().session_id();

        match action {
            ChatAction::Join { username } => {
                let username = username.trim();
                if username.is_empty()
                    || username.chars().count() > 32
                    || username.chars().any(char::is_control)
                {
                    Self::reject(ctx, "username must contain 1-32 printable characters").await;
                    return;
                }
                if self.members.contains_key(&session_id) {
                    Self::reject(ctx, "this Session already joined the room").await;
                    return;
                }
                if self.members.values().any(|member| member == username) {
                    Self::reject(ctx, "username is already in use").await;
                    return;
                }

                let username = username.to_owned();
                self.members.insert(session_id, username.clone());
                tracing::info!(room = self.runtime.actor_id(), %username, "member joined");
                self.broadcast(&ChatEvent::Joined { username }).await;
            }
            ChatAction::Send { text } => {
                let Some(username) = self.members.get(&session_id).cloned() else {
                    Self::reject(ctx, "join the room before sending messages").await;
                    return;
                };
                let text = text.trim();
                if text.is_empty()
                    || text.chars().count() > 1_000
                    || text.chars().any(char::is_control)
                {
                    Self::reject(ctx, "message must contain 1-1000 printable characters").await;
                    return;
                }

                tracing::info!(room = self.runtime.actor_id(), %username, %text, "message received");
                let event = ChatEvent::Message {
                    username,
                    text: text.to_owned(),
                };
                self.broadcast(&event).await;
            }
            ChatAction::Leave => {
                if let Some(username) = self.members.remove(&session_id) {
                    tracing::info!(room = self.runtime.actor_id(), %username, "member left");
                    self.broadcast(&ChatEvent::Left { username }).await;
                }
            }
        }
    }

    async fn on_session_closed(&mut self, ctx: &MessageContext) {
        if let Some(username) = self.members.remove(&ctx.session().session_id()) {
            tracing::info!(room = self.runtime.actor_id(), %username, "member disconnected");
            self.broadcast(&ChatEvent::Left { username }).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .with_writer(std::io::stdout)
        .init();
    tracing::info!(endpoint = "127.0.0.1:7000", "starting chat Server");

    Server::builder(coordination_store()?)
        .advertised_endpoint("127.0.0.1:7000")
        .node_id("chat-server")
        .actor::<ChatRoom>("chat-room")
        .shutdown_signal(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .serve("127.0.0.1:7000")
        .await?;

    tracing::info!("chat Server stopped");
    Ok(())
}
