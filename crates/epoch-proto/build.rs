//! Build script: compiles the control-plane gRPC contract when the `grpc`
//! feature is enabled. Without the feature the script is a no-op — it neither
//! references `tonic-prost-build` (an optional build dependency) nor invokes
//! `protoc`, so the pure data-plane crates build with no extra tooling.
//!
//! Design: docs/design/01-pd.md §7; docs/design/06-code-layout.md §1

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
