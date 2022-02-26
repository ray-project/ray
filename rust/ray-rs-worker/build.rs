use rustc_version::{version_meta, Channel};
use std::env;
use std::path::PathBuf;
use std::process::Command;

// It is the job of the downstream user binary to ensure that the linking is to the Ray Rust libraries
// are included in their rustc flags or via a build script like this one.
fn main() {
    let out = Command::new("ray")
        .arg("rust")
        .arg("--show-library-path")
        .output()
        .expect(
            "Could not get Ray Rust library path from stdout of `ray rust --show-library-path`",
        );

    let out_str = String::from_utf8(out.stdout).expect("Could not parse stdout as string");
    if !out_str.contains("/ray/rust/lib") {
        panic!("Could not get Ray Rust library path from stdout of `ray rust --show-library-path`");
    }
    let link_dir = out_str.split("--").collect::<Vec<_>>()[1]
        // Strip whitespace and newlines
        .replace('\n', "")
        .replace('\r', "")
        .replace(" ", "");

    println!("cargo:rustc-link-lib=razor");
    // println!("cargo:rustc-link-lib=core_worker_library_c");
    println!("cargo:rustc-link-search={}", link_dir);

    // println!(
    //     "cargo:rustc-env=LD_LIBRARY_PATH={}:LD_LIBRARY_PATH",
    //     link_dir
    // );

    let is_nightly = version_meta().expect("nightly check failed").channel == Channel::Nightly;
    if is_nightly {
        println!("cargo:rustc-cfg=nightly");
    }
}
