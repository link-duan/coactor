use std::io::BufRead;

use coactor::{ActorAddress, Client, Session};

#[path = "chat/protocol.rs"]
mod protocol;
#[path = "chat/storage.rs"]
mod storage;

use protocol::{ChatAction, ChatEvent, encode};
use storage::coordination_store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let room = arguments
        .next()
        .ok_or("usage: chat_client <room> <username>")?;
    let username = arguments
        .next()
        .ok_or("usage: chat_client <room> <username>")?
        .trim()
        .to_owned();
    if arguments.next().is_some() {
        return Err("usage: chat_client <room> <username>".into());
    }

    let client = Client::builder(coordination_store()?).build()?;

    let address = ActorAddress::new("chat-room", room)?;
    let mut session = client.open(&address).await?;
    join(&mut session, &username).await?;

    println!("Connected to {address}. Type /quit or press Ctrl+C to leave.");
    let result = run_chat(&mut session, &username).await;
    client.shutdown().await;
    result
}

async fn join(session: &mut Session, username: &str) -> Result<(), Box<dyn std::error::Error>> {
    session
        .send(encode(&ChatAction::Join {
            username: username.to_owned(),
        }))
        .await?;

    loop {
        let event = receive_event(session).await?;
        match &event {
            ChatEvent::Joined { username: member } if member == username => return Ok(()),
            ChatEvent::Error { message } => return Err(message.clone().into()),
            _ => print_event(event),
        }
    }
}

async fn run_chat(session: &mut Session, username: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut lines = stdin_lines();

    loop {
        tokio::select! {
            event = session.recv() => {
                match event {
                    Some(Ok(payload)) => print_event(serde_json::from_slice(&payload)?),
                    Some(Err(error)) => return Err(error.into()),
                    None => return Err("the Session ended".into()),
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return leave(session, username).await;
            }
            line = lines.recv() => {
                let Some(line) = line else {
                    return leave(session, username).await;
                };
                let line = line?;
                if line.trim() == "/quit" {
                    return leave(session, username).await;
                }
                if !line.trim().is_empty() {
                    session
                        .send(encode(&ChatAction::Send { text: line }))
                        .await?;
                }
            }
        }
    }
}

async fn leave(session: &mut Session, username: &str) -> Result<(), Box<dyn std::error::Error>> {
    session.send(encode(&ChatAction::Leave)).await?;
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        wait_for_leave_event(session, username),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err("timed out waiting for the leave Event".into()),
    }
}

async fn wait_for_leave_event(
    session: &mut Session,
    username: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let event = receive_event(session).await?;
        let left = matches!(&event, ChatEvent::Left { username: member } if member == username);
        print_event(event);
        if left {
            return Ok(());
        }
    }
}

async fn receive_event(session: &mut Session) -> Result<ChatEvent, Box<dyn std::error::Error>> {
    match session.recv().await {
        Some(Ok(payload)) => Ok(serde_json::from_slice(&payload)?),
        Some(Err(error)) => Err(error.into()),
        None => Err("the Session ended".into()),
    }
}

fn stdin_lines() -> tokio::sync::mpsc::UnboundedReceiver<std::io::Result<String>> {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    receiver
}

fn print_event(event: ChatEvent) {
    match event {
        ChatEvent::Joined { username } => println!("* {username} joined"),
        ChatEvent::Message { username, text } => println!("<{username}> {text}"),
        ChatEvent::Left { username } => println!("* {username} left"),
        ChatEvent::Error { message } => eprintln!("! {message}"),
    }
}
