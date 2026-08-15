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

//! epoch-ec (L1): erasure coding + bitrot framing.
//!
//! Pure computation: no I/O, no async runtime. `Erasure` encodes/reconstructs a
//! stripe's shards in memory; `frame` wraps each stored shard unit in a BLAKE3
//! bitrot frame; `layout` holds the striping math; `buffer` provides contiguous
//! per-stripe shard storage. I/O is injected by callers in higher layers.

pub mod buffer;
pub mod erasure;
pub mod frame;
pub mod layout;

pub use buffer::ShardBuffers;
pub use erasure::{EcError, Erasure};
pub use frame::FrameError;
