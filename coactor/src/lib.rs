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

pub use coactor_macros::{actor, command};

const PEER_PROTOCOL_VERSION: u32 = 1;

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
pub use runtime::{Runtime, RuntimeBuilder};

#[cfg(test)]
#[path = "../tests/support/http_fixture.rs"]
mod http_fixture;

#[cfg(test)]
#[path = "../tests/actor_ownership.rs"]
mod actor_ownership_tests;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/counter_call_path.rs"]
mod counter_call_path_tests;
#[cfg(test)]
#[path = "../tests/counter_vertical_slice.rs"]
mod counter_vertical_slice_tests;
#[cfg(test)]
#[path = "../tests/failure_semantics.rs"]
mod failure_semantics_tests;
#[cfg(test)]
#[path = "../tests/mailbox_semantics.rs"]
mod mailbox_semantics_tests;
#[cfg(test)]
#[path = "../tests/node_authority.rs"]
mod node_authority_tests;
#[cfg(test)]
#[path = "../tests/passivation_semantics.rs"]
mod passivation_semantics_tests;
#[cfg(test)]
#[path = "../tests/remote_call_path.rs"]
mod remote_call_path_tests;
#[cfg(test)]
#[path = "../tests/s3_ownership_contract.rs"]
mod s3_ownership_contract_tests;
#[cfg(test)]
#[path = "../tests/shutdown_semantics.rs"]
mod shutdown_semantics_tests;
#[cfg(test)]
#[path = "../tests/tracing_semantics.rs"]
mod tracing_semantics_tests;
