use std::env;
use std::path::PathBuf;

// This should be an environment variable in the future.
// const RAY_RUST_LINK_DIR: &'static str = "/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib";

fn main() {
    // Tell cargo to tell rustc to link the core_worker_library_rust
    // shared library.
    //
    // In the future, we need this to be the location in the ray pkg
    // Use environment variable

    let link_dir = env::var("RAY_RUST_LIBRARY_DIR").unwrap();

    println!("cargo:rustc-link-lib=core_worker_library_c");
    println!("cargo:rustc-link-search={}", link_dir);
    println!("cargo:rustc-cdylib-link-args=-Wl,-export-dynamic -nostartfiles");

    println!("cargo:rustc-env=LD_LIBRARY_PATH={}:LD_LIBRARY_PATH", link_dir);

    let mut out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    out_dir.pop();
    out_dir.pop();
    out_dir.pop();

    println!(
        "cargo:rustc-env=RAY_RUST_LIBRARY_PATHS=--ray_code_search_path={}/libsimple.so",
        out_dir.to_str().unwrap(),
    );
}
