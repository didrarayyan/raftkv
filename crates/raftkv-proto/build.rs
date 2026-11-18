//! Build script compiling `proto/raftkv.proto` with the vendored `protoc` binary.

#![forbid(unsafe_code)]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/raftkv.proto");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);
    tonic_prost_build::configure()
        .bytes(".")
        .compile_with_config(config, &["proto/raftkv.proto"], &["proto"])?;
    Ok(())
}
