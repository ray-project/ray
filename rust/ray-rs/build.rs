use rustc_version::{version_meta, Channel};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let is_nightly = version_meta().expect("nightly check failed").channel == Channel::Nightly;
    if is_nightly {
        println!("cargo:rustc-cfg=nightly");
    }
}
