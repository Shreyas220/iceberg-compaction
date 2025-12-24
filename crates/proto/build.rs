/*
 * Build script for compiling protobuf definitions using tonic-build.
 */

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the proto file with tonic-build
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(&["proto/compaction.proto"], &["proto"])?;

    // Tell Cargo to rerun this build script if the proto file changes
    println!("cargo:rerun-if-changed=proto/compaction.proto");

    Ok(())
}
