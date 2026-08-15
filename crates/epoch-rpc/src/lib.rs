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

//! epoch-rpc (L1): custom binary RPC for the shard data plane.
//!
//! 50-byte fixed header + pcode dispatch + stream multiplexing; includes an
//! in-process [`LocalTransport`] so a gateway can reach a co-located store
//! without the network stack. Zero-copy on the hot path: bodies flow as
//! [`bytes::Bytes`] and are written vectored with their frame header.

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
