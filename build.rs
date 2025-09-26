use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path().unwrap();
    std::env::set_var("PROTOC", protoc);
    
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Look for proto files in the submodule directory
    let proto_dir = Path::new("proto/borealis-prototypes");

    if !proto_dir.exists() {
        eprintln!("Error: borealis-prototypes submodule not found!");
        eprintln!("Please run: git submodule update --init --recursive");
        eprintln!("Or run ./setup.sh to set up the submodule");
        std::process::exit(1);
    }

    // Find all .proto files recursively in the submodule
    let mut proto_files = Vec::new();
    find_proto_files(proto_dir, &mut proto_files)?;

    if proto_files.is_empty() {
        eprintln!("Error: No .proto files found in borealis-prototypes submodule!");
        eprintln!("Directory contents:");
        if let Ok(entries) = fs::read_dir(proto_dir) {
            for entry in entries.flatten() {
                eprintln!("  - {:?}", entry.path());
            }
        }
        eprintln!("Please run: git submodule update --recursive");
        std::process::exit(1);
    }

    println!("Found proto files: {:?}", proto_files);

    // Configure tonic-build for the actual Aurora Borealis proto files
    let builder = tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("blocksapi_descriptor.bin"));

    builder.compile_protos(
        &proto_files,
        &["proto/borealis-prototypes".to_string()], // Include the submodule directory
    )?;

    // Tell cargo to recompile if proto files change
    println!("cargo:rerun-if-changed=proto/borealis-prototypes/");
    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}

fn find_proto_files(dir: &Path, proto_files: &mut Vec<String>) -> std::io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                find_proto_files(&path, proto_files)?;
            } else if let Some(extension) = path.extension() {
                if extension == "proto" {
                    if let Some(path_str) = path.to_str() {
                        proto_files.push(path_str.to_string());
                    }
                }
            }
        }
    }
    Ok(())
}
