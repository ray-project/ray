#[cfg(test)]
mod test {
    use actor::*;
    use ray_rs::{
        get, // put,
        load_libraries_from_paths,
        rust_worker_execute,
        *,
    };
    use std::sync::Mutex;

    // Move this into test utils?
    const NUM_CLUSTER_TESTS: usize = 1;

    lazy_static! {
        static ref CLUSTER_TEST_COUNTER: Mutex<(usize, usize)> = Mutex::new((0, 0));
    }

    fn try_init() {
        let mut guard = CLUSTER_TEST_COUNTER.lock().unwrap();
        // TODO: get rid of this ugly monkey patch code
        if guard.0 == 0 {
            let env_var = std::env::var("RAY_RUST_LIBRARY_PATHS").unwrap();
            let mut args = vec![
                CString::new("").unwrap(),
                CString::new("--ray_code_search_path=").unwrap(),
            ];

            if env_var.starts_with("--ray_code_search_path=") {
                args[1] = CString::new(env_var.clone()).unwrap();
                let (_, path_str) = env_var.split_at("--ray_code_search_path=".len());
                let paths = path_str.split(":").collect::<Vec<&str>>();
                println!("{:?}", paths);
                load_libraries_from_paths(&paths);
            }
            let c_args = args
                .iter()
                .map(|arg| arg.as_ptr())
                .collect::<Vec<*const std::os::raw::c_char>>();
            ray::init_inner(
                true,
                Some(rust_worker_execute),
                Some(internal::set_async_result),
                Some((c_args.len() as std::os::raw::c_int, c_args.as_ptr())),
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

    // #[test]
    // fn test_create_vec2() {
    //     try_init();
    //     {
    //         let handle = new_vec2.remote(4, 5);
    //
    //         let obj_ref = get_vec2.remote(&handle);
    //         let obj = get(&obj_ref);
    //
    //         println!("{:?}", obj);
    //         let obj_ref_0 = add_assign_vec2.remote(&handle, new_vec2.call(5, 4));
    //         let obj_0 = get(&obj_ref_0);
    //         println!("{:?}", obj_0);
    //
    //         let obj_ref = get_vec2.remote(&handle);
    //         let obj = get(&obj_ref);
    //
    //         println!("{:?}", obj);
    //     }
    //     try_shutdown();
    // }

    // #[test]
    // fn test_append_string() {
    //     try_init();
    //     {
    //         let handle = new_string.remote_async(String::from("Hello"));
    //
    //         // let obj_ref_0 = append_stateless.remote(String::from("Hello"), String::from(" World"));
    //         // let obj_0 = get(&obj_ref_0);
    //         // println!("{:?}", obj_0);
    //         let obj_ref = append.remote(&handle, String::from(" World"));
    //
    //         let obj = get(&obj_ref);
    //     }
    //     try_shutdown();
    // }

    // #[test]
    // fn test_append_string_async() {
    //     try_init();
    //     {
    //         // Remote async with more than one thread requires
    //         let handle = new_string_threadsafe.remote_async(String::from("Hello"));
    //
    //         // let obj_ref_0 = append_stateless.remote(String::from("Hello"), String::from(" World"));
    //         // let obj_0 = get(&obj_ref_0);
    //         // println!("{:?}", obj_0);
    //         let now = std::time::Instant::now();
    //         // Next step: define tasks with closures etc...
    //         let mut obj_refs: Vec<_> = (0..10)
    //             .map(|i| append_threadsafe.remote(&handle, format!(" World X {}", i)))
    //             .collect();
    //
    //         obj_refs.iter().for_each(|obj_ref| {
    //             let obj = get(&obj_ref);
    //             println!("{:?} {:?}", obj, now.elapsed().as_millis());
    //         });
    //
    //         let obj = get(&append_threadsafe.remote(&handle, format!(" World X {}", "MAX")));
    //         println!("{:?} {:?}", obj, now.elapsed().as_millis());
    //     }
    //     try_shutdown();
    // }

    // #[test]
    // fn test_append_string_tokio() {
    //     try_init();
    //     {
    //         // Remote async with more than one thread requires
    //         let handle = new_string_tokio.remote_async(String::from("Hello"));
    //
    //         let now = std::time::Instant::now();
    //         // Next step: define tasks with closures etc...
    //         let mut obj_refs: Vec<_> = (0..0)//100_000)
    //             .map(|i| append_tokio.remote(&handle, format!(" World X {}", i)))
    //             .collect();
    //
    //         obj_refs.iter().for_each(|obj_ref| {
    //             let obj = get(&obj_ref);
    //             // println!("{:?} {:?}", obj, now.elapsed().as_millis());
    //         });
    //
    //         let obj = get(&append_tokio.remote(&handle, format!(" World X {}", "MAX")));
    //         println!("{:?} {:?}", obj, now.elapsed().as_millis());
    //     }
    //     try_shutdown();
    // }

    #[tokio::test]
    async fn test_append_string_tokio_get_async() {
        try_init();
        {
            // Remote async with more than one thread requires
            let handle = new_string_tokio.remote_async(String::from("Hello"));

            let now = std::time::Instant::now();
            // Next step: define tasks with closures etc...
            let mut obj_refs: Vec<_> = (0..0)//100_000)
                .map(|i| append_tokio.remote(&handle, format!(" World X {}", i)))
                .collect();

            obj_refs.iter().for_each(|obj_ref| {
                // let obj = get_async(&obj_ref);
                // println!("{:?} {:?}", obj, now.elapsed().as_millis());
            });

            let now = std::time::Instant::now();
            let id = append_tokio.remote(&handle, format!(" World X {}", "MAX"));
            let obj = get_async(&id);
            println!(
                "trying to get: {:?}. [{} ms elapsed so far]",
                id,
                now.elapsed().as_millis()
            );

            println!("{:?} {:?}", obj.await, now.elapsed().as_millis());
        }
        try_shutdown();
    }
}
