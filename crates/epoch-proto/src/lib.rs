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

//! epoch-proto (L0): the workspace vocabulary — ID types, error codes, constants.
//!
//! The single source of truth for identity encodings and cross-component error
//! codes (docs/design/00-overview.md §2 is the terminology authority). Every
//! other crate depends on this one; it depends on no workspace crate.
//!
//! Design: docs/design/00-overview.md §2/§4; docs/design/06-code-layout.md §1
//!
//! M0 delivers `id` / `error` / `consts`; the `grpc` feature adds the
//! control-plane gRPC contract (`grpc::pd`, generated from `src/grpc/pd.proto`),
//! compiled only by PD and client crates.

pub mod code_mode;
pub mod consts;
pub mod error;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod id;

pub use code_mode::{CodeMode, CodeModeId};
pub use error::EpochError;
pub use id::{
    BlobId, BucketId, ChunkId, DiskId, ExtentId, NodeId, PartitionId, ShardId, WriterToken,
};
