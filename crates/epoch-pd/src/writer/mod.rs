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

//! Writer registry: PD-issued writer tokens and their session liveness.
//!
//! [`WriterManager`] owns the replicated token records; [`model`] holds the
//! record / status / commands; [`liveness`] derives session retirement from
//! gateway heartbeat staleness. A token is the high 32 bits of every `blob_id`
//! (00 §4), issued from a monotonic counter, never reused, and one-way
//! `Live → Dead` (01 §4.3).

pub mod liveness;
pub mod manager;
pub mod model;

pub use liveness::DEFAULT_WRITER_DEAD_AFTER_MILLIS;
pub use manager::WriterManager;
pub use model::{MarkWriterDead, RegisterWriter, WriterRecord, WriterStatus};

pub(crate) use liveness::dead_writer_plan;
