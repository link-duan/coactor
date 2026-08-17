extern crate self as coactor;

pub mod cluster;

#[cfg(test)]
pub(crate) use cluster::PlacementCtx;
#[cfg(test)]
pub(crate) use cluster::S3CoordinationStore;
#[cfg(test)]
pub(crate) use cluster::ServerRuntimeConfig;
pub(crate) use cluster::{
    ActorOwner, ActorOwnerRecord, ActorOwnerStore, AmbiguousMutation, CoordinationError,
    CoordinationStores, LeaseMutation, LeaseTiming, LeaseToken, Mutation, NodeDirectory,
    NodeLeaseStore, NodeRecord, NodeSessionId, PlacementStrategy, Revision, ServerConfig,
    ServerStarter, ServerSupervision, ServerTermination, ServerTerminationReason,
    VersionedActorOwnerRecord,
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
pub use client::session::Session;
pub use client::{ActorRef, Client, ClientBuilder, ClientConfig};
pub use cluster::{CoordinationConfig, S3CoordinationConfig};
pub use runtime::actor::{Actor, ActorRuntime, BoxFuture, MessageContext};
pub use runtime::session::{SessionHandle, SessionId};
pub use runtime::{Server, ServerBuilder};
pub use transport::Endpoint;

#[cfg(test)]
#[path = "../tests/cluster_authority.rs"]
mod cluster_authority_tests;
#[cfg(test)]
#[path = "../tests/cluster_session.rs"]
mod cluster_session_tests;
#[cfg(test)]
#[path = "../tests/placement.rs"]
mod placement_tests;
#[cfg(test)]
#[path = "../tests/s3_coordination.rs"]
mod s3_coordination_tests;
#[cfg(test)]
#[path = "../tests/session_semantics.rs"]
mod session_semantics_tests;
