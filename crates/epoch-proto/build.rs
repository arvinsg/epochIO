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

//! Build script: compiles the control-plane gRPC contract when the `grpc`
//! feature is enabled. Without the feature the script is a no-op — it neither
//! references `tonic-prost-build` (an optional build dependency) nor invokes
//! `protoc`, so the pure data-plane crates build with no extra tooling.

fn main() {
    #[cfg(feature = "grpc")]
    {
        println!("cargo:rerun-if-changed=src/grpc/pd.proto");
        println!("cargo:rerun-if-changed=src/grpc/raft.proto");
        println!("cargo:rerun-if-changed=src/grpc/meta.proto");
        tonic_prost_build::configure()
            .compile_protos(
                &[
                    "src/grpc/pd.proto",
                    "src/grpc/raft.proto",
                    "src/grpc/meta.proto",
                ],
                &["src/grpc"],
            )
            .expect("compile src/grpc/*.proto (requires protoc on PATH)");
    }
}
