use ray_rs_sys::*;
use std::os::raw::*;
use std::ffi::CString;

// Could you just build this as a C library...?
// Maybe not as you still do want to register the language-specific callback internally...
fn main() {
    // wrangle rust env strings as argc and argv
    let args = std::env::args().map(|arg| CString::new(arg).unwrap() ).collect::<Vec<CString>>();
    let c_args = args.iter().map(|arg| arg.as_ptr()).collect::<Vec<*const c_char>>();

    ray::init_inner(
        false,
        Some(rust_worker_execute_add),
        Some((
            c_args.len() as c_int,
            c_args.as_ptr()
        ))
    );
    ray::run();
    ray::shutdown();
}
