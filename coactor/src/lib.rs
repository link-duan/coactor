//! Embedded distributed Actor runtime for Rust applications.
//!
//! CoActor addresses logical Actors with a validated [`ActorAddress`], hosts them
//! in a [`Server`], and lets callers communicate through bidirectional
//! [`Session`]s opened by a [`Client`]. Actions and Events are byte messages with
//! in-memory, at-most-once delivery semantics.
//!
//! Availability failover starts a replacement Active Actor with empty CoActor-managed state.
//!
//! Start with the repository's [Getting Started guide][getting-started].
//!
//! [getting-started]: https://github.com/link-duan/coactor/blob/main/docs/getting-started.md

extern crate self as coactor;

mod cluster;

pub mod coordination {
    pub use crate::cluster::{
        ActorOwner, ActorOwnerRecord, ActorOwnerStore, CoordinationError, CoordinationErrorKind,
        CoordinationStore, MutationOutcome, NodeDirectory, NodeLeaseStore, NodeRecord,
        NodeSessionId, Revision, VersionedActorOwnerRecord,
    };

    pub mod backend {
        pub mod s3 {
            pub use crate::cluster::s3::{S3CoordinationStore, S3StoreConfigError};
        }
    }
}

#[cfg(test)]
pub(crate) use cluster::s3::S3CoordinationStore;
pub(crate) use cluster::{
    ActorOwner, ActorOwnerRecord, ActorOwnerStore, CoordinationError, MutationOutcome,
    NodeDirectory, NodeLeaseStore, NodeRecord, NodeSessionId, PlacementStrategy, Revision,
    VersionedActorOwnerRecord,
};
#[cfg(test)]
pub(crate) use cluster::{CoordinationStores, ServerRuntimeConfig, ServerStarter};
#[cfg(test)]
pub(crate) use runtime::ServerBuilderCore;

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
pub use client::{Client, ClientBuilder};
pub use runtime::actor::{Actor, ActorRuntime, MessageContext};
pub use runtime::session::{SessionHandle, SessionId};
pub use runtime::{Server, ServerBuilder};

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
