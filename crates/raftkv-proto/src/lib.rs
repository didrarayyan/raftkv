//! Generated protobuf and gRPC wire definitions for `raftkv`.
//!
//! Role: compiles `proto/raftkv.proto` at build time using a vendored `protoc`
//! binary and re-exports the generated client, server, and message types.
//!
//! Invariants:
//! - Wire types contain zero business logic.
//!
//! Deliberately omitted:
//! - Domain conversions, validation, or Raft protocol semantics.

#![forbid(unsafe_code)]

// Generated tonic/prost code does not include doc comments or satisfy all pedantic lints.
#[allow(
    missing_docs,
    unreachable_pub,
    clippy::all,
    clippy::pedantic,
    reason = "prost and tonic codegen emits undocumented items and internal stub modules"
)]
mod generated {
    tonic::include_proto!("raftkv.v1");
}

pub use generated::*;
