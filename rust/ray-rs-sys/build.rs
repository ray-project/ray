extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to tell rustc to link the core_worker_library_rust
    // shared library.
    //
    // In the future, we need this to be the location in the ray pkg
    // Use environment variable

    // println!("cargo:rustc-link-lib=core_worker_library_c");
    // println!("cargo:rustc-link-search=/home/jonch/.cache/bazel/_bazel_jonch/3819ce4da5bb63a59f7ecd1a722a08d9/execroot/com_github_ray_project_ray/bazel-out/k8-opt/bin");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=../../src/core_worker/lib/c/c_worker.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("../../src/ray/core_worker/lib/c/c_worker.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("ray_rs_sys_bindgen.rs"))
        .expect("Couldn't write bindings!");
}
