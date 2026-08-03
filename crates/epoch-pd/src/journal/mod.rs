//! The journal: the single entry point for proposing replicated PD mutations.
//!
//! All state changes flow through [`Journal::propose`], which submits a
//! [`PdEntry`] to the raft group and returns the applied [`ApplyResult`]. The
//! journal owns the running raft node; domain managers (cluster / chunk / …)
//! propose through it in later phases.
//!
//! Design: docs/design/01-pd.md §2

pub mod client;
pub mod entry;

pub use client::{Journal, WriterLivenessHandle};
pub use entry::{ApplyResult, PdEntry};
