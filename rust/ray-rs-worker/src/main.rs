// TODO: Load as prelude?
use ray_rs::{load_code_paths_from_raw_c_cmdline, ray, rust_worker_execute_async};
use std::ffi::CString;
use std::os::raw::*;

// Could you just build this as a C library...?
// Maybe not as you still do want to register the language-specific callback internally...
fn main() {
    // wrangle rust env strings as argc and argv
    let args = std::env::args()
        .map(|arg| CString::new(arg).unwrap())
        .collect::<Vec<CString>>();
    let c_args = args
        .iter()
        .map(|arg| arg.as_ptr())
        .collect::<Vec<*const c_char>>();

    load_code_paths_from_raw_c_cmdline(c_args.len() as c_int, c_args.as_ptr() as *mut *mut i8);

    ray::init_inner(
        false,
        Some(rust_worker_execute_async),
        Some((c_args.len() as c_int, c_args.as_ptr())),
    );
    ray::run();
    ray::shutdown();
}
