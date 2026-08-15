// Copyright 2026 arvinsg
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The journal: the single entry point for proposing replicated PD mutations.
//!
//! All state changes flow through [`Journal::propose`], which submits a
//! [`PdEntry`] to the raft group and returns the applied [`ApplyResult`]. The
//! journal owns the running raft node; domain managers (cluster / chunk / …)
//! propose through it in later phases.

pub mod client;
pub mod entry;

pub use client::{Journal, WriterLivenessHandle};
pub use entry::{ApplyResult, PdEntry};
