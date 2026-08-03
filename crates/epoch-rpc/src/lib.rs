//! epoch-rpc (L1): custom binary RPC for the shard data plane.
//!
//! 50-byte fixed header + pcode dispatch + stream multiplexing; includes an
//! in-process [`LocalTransport`] so a gateway can reach a co-located store
//! without the network stack. Zero-copy on the hot path: bodies flow as
//! [`bytes::Bytes`] and are written vectored with their frame header.
//!
//! Design: docs/design/02-datanode.md §5; docs/design/06-code-layout.md §4

pub mod codec;
pub mod frame;

mod conn;
mod pool;
mod server;
mod transport;

pub use codec::{
    CodecError, CreateExtentReq, CreateExtentResp, DeleteBlobReq, DeleteBlobResp, EndReq,
    ErrorResp, FLAG_READ_HIT, ListBlobsReq, ListBlobsResp, OpenReq, Pcode, ReadShardReq, SealReq,
    SealResp,
};
pub use frame::{FRAME_HEADER_LEN, FrameError, FrameHeader, PROTOCOL_VERSION};
pub use pool::RemoteTransport;
pub use server::Server;
pub use transport::{LocalTransport, ShardHandler, ShardTransport, WriteStream};
