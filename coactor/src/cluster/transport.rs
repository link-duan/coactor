use std::{sync::Arc, time::Duration};

use tonic::{Request, Response, Status};

use crate::{
    __macro::{RemotePayload, RuntimeError, RuntimeInner},
    ActorAddress, peer_protocol,
};

pub(crate) async fn invoke_peer(
    address: &ActorAddress,
    endpoint: String,
    protocol_version: u32,
    command: &'static str,
    payload: Vec<u8>,
    connect_timeout: Option<Duration>,
) -> Result<RemotePayload, RuntimeError> {
    let connect = peer_protocol::peer_client::PeerClient::connect(endpoint);
    let mut client = match connect_timeout {
        Some(timeout) => tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| RuntimeError::RemoteUnavailable)?
            .map_err(|_| RuntimeError::RemoteUnavailable)?,
        None => connect.await.map_err(|_| RuntimeError::RemoteUnavailable)?,
    };
    let response = client
        .invoke(peer_protocol::InvokeRequest {
            protocol_version,
            actor_type: address.actor_type().to_owned(),
            actor_id: address.actor_id().as_bytes().to_vec(),
            command: command.to_owned(),
            payload,
        })
        .await
        .map_err(|_| RuntimeError::OutcomeUnknown)?
        .into_inner();
    use peer_protocol::invoke_response::Outcome;
    match response.outcome {
        Some(Outcome::Success(bytes)) => Ok(RemotePayload::Success(bytes)),
        Some(Outcome::HandlerError(bytes)) => Ok(RemotePayload::HandlerError(bytes)),
        Some(Outcome::RuntimeFailure(failure)) => Err(RuntimeError::from_wire(failure)),
        None => Err(RuntimeError::RemoteProtocol),
    }
}

pub(crate) struct PeerService<S> {
    pub(crate) runtime: Arc<RuntimeInner<S>>,
}

#[tonic::async_trait]
impl<S> peer_protocol::peer_server::Peer for PeerService<S>
where
    S: Send + Sync + 'static,
{
    async fn invoke(
        &self,
        request: Request<peer_protocol::InvokeRequest>,
    ) -> Result<Response<peer_protocol::InvokeResponse>, Status> {
        let request = request.into_inner();
        let outcome = if request.protocol_version != self.runtime.peer_protocol_version {
            runtime_failure(RuntimeError::RemoteProtocol)
        } else {
            match self
                .runtime
                .dispatch_peer(
                    request.actor_type.as_str(),
                    request.actor_id,
                    request.command.as_str(),
                    request.payload,
                )
                .await
            {
                Ok(RemotePayload::Success(bytes)) => {
                    Some(peer_protocol::invoke_response::Outcome::Success(bytes))
                }
                Ok(RemotePayload::HandlerError(bytes)) => {
                    Some(peer_protocol::invoke_response::Outcome::HandlerError(bytes))
                }
                Err(error) => runtime_failure(error),
            }
        };
        Ok(Response::new(peer_protocol::InvokeResponse { outcome }))
    }
}

fn runtime_failure(error: RuntimeError) -> Option<peer_protocol::invoke_response::Outcome> {
    Some(peer_protocol::invoke_response::Outcome::RuntimeFailure(
        error.to_wire(),
    ))
}
