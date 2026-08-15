extern crate self as coactor;

pub mod cluster;

#[cfg(test)]
pub(crate) use cluster::ServerRuntimeConfig;
pub(crate) use cluster::{
    ActorOwner, ActorOwnerRecord, AmbiguousMutation, ServerConfig, ServerStarter, LeaseMutation,
    LeaseTiming, NodeLease, NodeSessionId, OwnershipBackend, OwnershipBackendError,
    ServerSupervision, ServerTermination, ServerTerminationReason, S3OwnershipBackend,
    S3OwnershipConfig, VersionedActorOwnerRecord, VersionedNodeLease,
};

pub use coactor_macros::actor;

const PEER_PROTOCOL_VERSION: u32 = 2;

mod peer_protocol {
    tonic::include_proto!("coactor.peer.v1");
}

#[doc(hidden)]
#[path = "macro_support.rs"]
pub mod __macro;
mod actor;
mod client;
mod runtime;
mod transport;

pub mod test_support;

pub use actor::*;
pub use client::discovery::{DnsDiscovery, StaticListDiscovery, DiscoveryError, ServiceDiscovery};
pub use client::{ActorRef, Client, ClientBuilder, ClientConfig};
pub use client::session::Session;
pub use transport::Endpoint;
pub use runtime::actor::{Actor, ActorRuntime, BoxFuture, MessageContext};
pub use runtime::session::{SessionHandle, SessionId};
pub use runtime::{Server, ServerBuilder};

#[cfg(test)]
#[path = "../tests/session_semantics.rs"]
mod session_semantics_tests;
#[cfg(test)]
#[path = "../tests/cluster_session.rs"]
mod cluster_session_tests;
#[cfg(test)]
#[path = "../tests/cluster_authority.rs"]
mod cluster_authority_tests;
