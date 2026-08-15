extern crate self as coactor;

pub mod cluster;

#[cfg(test)]
pub(crate) use cluster::ClusterRuntimeConfig;
pub(crate) use cluster::{
    ActorOwner, ActorOwnerRecord, AmbiguousMutation, ClusterConfig, ClusterStarter, LeaseMutation,
    LeaseTiming, NodeLease, NodeSessionId, OwnershipBackend, OwnershipBackendError,
    RuntimeSupervision, RuntimeTermination, RuntimeTerminationReason, S3OwnershipBackend,
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
mod runtime;
#[cfg(test)]
mod test_support;

pub use actor::*;
pub use runtime::actor::{Actor, ActorRuntime, BoxFuture, MessageContext};
pub use runtime::core::ActorRef;
pub use runtime::session::{Session, SessionHandle, SessionId};
pub use runtime::{Runtime, RuntimeBuilder};

#[cfg(test)]
#[path = "../tests/session_semantics.rs"]
mod session_semantics_tests;
#[cfg(test)]
#[path = "../tests/cluster_session.rs"]
mod cluster_session_tests;
#[cfg(test)]
#[path = "../tests/cluster_authority.rs"]
mod cluster_authority_tests;
