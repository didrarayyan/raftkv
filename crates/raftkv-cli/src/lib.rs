//! Client library and CLI bindings for `raftkv`.
//!
//! Role: provides an async gRPC client with automatic leader discovery, hint
//! following, and idempotent write retries using stable `(client_id, request_id)`
//! pairs across retries.
//!
//! Invariants:
//! - Write requests keep the same `request_id` across retries and only advance it
//!   after a definitive response.
//! - `INVALID_ARGUMENT` responses are never retried.
//!
//! Deliberately omitted:
//! - Server internals, consensus logic, or local storage access.

#![forbid(unsafe_code)]
