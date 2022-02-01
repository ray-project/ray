use ray_rs::{*,
    get,// put,
    rust_worker_execute, load_libraries_from_paths,
};
use actor::*;

use std::sync::Mutex;

const NUM_CLUSTER_TESTS: usize = 1;

lazy_static! {
    static ref CLUSTER_TEST_COUNTER: Mutex<(usize, usize)> = Mutex::new((0, 0));
}

fn try_init() {
    let mut guard = CLUSTER_TEST_COUNTER.lock().unwrap();
    // TODO: get rid of this ugly monkey patch code
    if guard.0 == 0 {
        let env_var = std::env::var("RAY_RUST_LIBRARY_PATHS").unwrap();
        let mut args = vec![CString::new("").unwrap(), CString::new("--ray_code_search_path=").unwrap()];

        if env_var.starts_with("--ray_code_search_path=") {
            args[1] = CString::new(env_var.clone()).unwrap();
            let (_, path_str) = env_var.split_at("--ray_code_search_path=".len());
            let paths = path_str.split(":").collect::<Vec<&str>>();
            println!("{:?}", paths);
            load_libraries_from_paths(&paths);
        }
        let c_args = args.iter().map(|arg| arg.as_ptr()).collect::<Vec<*const std::os::raw::c_char>>();
        ray::init_inner(
            true,
            Some(rust_worker_execute),
            Some((
                c_args.len() as std::os::raw::c_int,
                c_args.as_ptr()
            )),
        );
    }
    guard.0 += 1;
}

fn try_shutdown() {
    let mut guard = CLUSTER_TEST_COUNTER.lock().unwrap();
    guard.1 += 1;
    if guard.1 == NUM_CLUSTER_TESTS {
        println!("shutting down");
        ray::shutdown()
    }
}


#[test]
fn test_create_vec2() {
    try_init();
    new.create_actor(4., 5.);
    try_shutdown();
}
