use super::*;
use crate::node_authority::{confirm_node_lease, wall_time_millis};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use tokio::sync::OwnedSemaphorePermit;

pub use futures_util::FutureExt;
pub use prost;
pub use tokio;

pub const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const STOPPED: u8 = 2;
const FENCED: u8 = 3;

mod authority;
mod runtime;

pub use authority::*;
pub use runtime::*;
