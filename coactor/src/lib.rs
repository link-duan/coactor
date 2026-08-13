extern crate self as coactor;

mod node_authority;
mod s3_node_lease;

use std::{
    any::Any,
    collections::HashMap,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
};

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

pub use node_authority::{
    ActorOwner, ActorOwnerRecord, ActorOwnerStorage, AmbiguousMutation, DistributedRuntimeBuilder,
    DistributedRuntimeConfig, LeaseMutation, LeaseTiming, NodeLease, NodeLeaseStorage,
    NodeSessionId, OwnershipStorage, OwnershipStorageError, RuntimeStartError, RuntimeSupervision,
    RuntimeTermination, RuntimeTerminationReason, VersionedActorOwnerRecord, VersionedNodeLease,
};
pub use s3_node_lease::{S3NodeLeaseConfig, S3NodeLeaseStorage};

pub use coactor_macros::{actor, command};

const PEER_PROTOCOL_VERSION: u32 = 1;

mod peer_protocol {
    tonic::include_proto!("coactor.peer.v1");
}

#[doc(hidden)]
pub mod __private;
mod api;

pub use api::*;
