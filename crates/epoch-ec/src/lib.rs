//! epoch-ec (L1): erasure coding + bitrot framing.
//!
//! Pure computation: no I/O, no async runtime. `Erasure` encodes/reconstructs a
//! stripe's shards in memory; `frame` wraps each stored shard unit in a BLAKE3
//! bitrot frame; `layout` holds the striping math; `buffer` provides contiguous
//! per-stripe shard storage. I/O is injected by callers in higher layers.
//!
//! Design: docs/design/04-ec-io.md; docs/design/06-code-layout.md §3

pub mod buffer;
pub mod erasure;
pub mod frame;
pub mod layout;

pub use buffer::ShardBuffers;
pub use erasure::{EcError, Erasure};
pub use frame::FrameError;
