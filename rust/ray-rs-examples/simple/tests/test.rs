#[cfg(test)]
mod test {
    use ray_rs::{
        get, // put,
        load_libraries_from_paths,
        rust_worker_execute,
        *,
    };
    use simple::*;
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
            let mut args = vec![
                CString::new("").unwrap(),
                CString::new("--code_search_path=").unwrap(),
            ];

            if env_var.starts_with("--code_search_path=") {
                args[1] = CString::new(env_var.clone()).unwrap();
                let (_, path_str) = env_var.split_at("--code_search_path=".len());
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
    // fn test_init_submit_execute_shutdown() {
    //     try_init();
    //     const VEC_SIZE: usize = 1 << 12;
    //     let num_jobs = 1 << 0;
    //
    //     let (a, b): (Vec<_>, Vec<_>) =
    //         ((0u64..VEC_SIZE as u64).collect(), (0u64..VEC_SIZE as u64).collect());
    //
    //     let now = std::time::Instant::now();
    //     println!("{}", add_two_vecs.name());
    //     let mut ids: Vec<_> = (0..num_jobs).map(|_| {
    //         add_two_vecs.remote(&a, &b)
    //     }).collect();
    //
    //     ids.reverse();
    //     println!("Submission: {:?}", now.elapsed().as_millis());
    //
    //     let results: Vec<_> = (0..num_jobs).map(|_| {
    //         get::<Vec<u64>>(ids.pop().unwrap())
    //     }).collect();
    //
    //     println!("Execute + Get: {:?}", now.elapsed().as_millis());
    //     try_shutdown();
    // }

    #[test]
    fn test_nested_remote() {
        try_init();
        const VEC_SIZE: usize = 1 << 12;
        let num_jobs = 1 << 0;
        let (a, b): (Vec<_>, Vec<_>) = (
            (0u64..VEC_SIZE as u64).collect(),
            (0u64..VEC_SIZE as u64).collect(),
        );

        let now = std::time::Instant::now();
        {
            let mut ids: Vec<_> = (0..num_jobs)
                .map(|_| add_two_vecs_nested.remote(&a, &b))
                .collect();

            ids.reverse();
            println!("Submission: {:?}", now.elapsed().as_millis());

            let _results: Vec<_> = (0..num_jobs)
                .map(|_| {
                    let obj_ref = ids.pop().unwrap();
                    (get::<Vec<u64>>(&obj_ref), get::<Vec<u64>>(&obj_ref))
                })
                .collect();
        }

        println!("Execute + Get: {:?}", now.elapsed().as_millis());
        try_shutdown();
    }

    // #[test]
    // fn test_nested_remote_outer_get() {
    //     try_init();
    //     const VEC_SIZE: usize = 1 << 20;
    //     let num_jobs = 1 << 0;
    //     let (a, b): (Vec<_>, Vec<_>) =
    //         ((0u64..VEC_SIZE as u64).collect(), (0u64..VEC_SIZE as u64).collect());
    //
    //     let now = std::time::Instant::now();
    //     let mut ids: Vec<_> = (0..num_jobs).map(|_| {
    //         add_two_vecs_nested_remote_outer_get.remote(&a, &b)
    //     }).collect();
    //
    //     ids.reverse();
    //     println!("Submission: {:?}", now.elapsed().as_millis());
    //
    //     let results: Vec<_> = (0..num_jobs).map(|_| {
    //         let byte_vec = get::<Vec<u8>>(ids.pop().unwrap());
    //         println!("{}", &byte_vec
    //             .iter()
    //             .map(|x| format!("{:02x?}", x))
    //             .collect::<Vec<_>>()
    //             .join("")
    //         );
    //         get::<Vec<u64>>(
    //             unsafe {
    //                 CString::from_vec_unchecked(byte_vec)
    //             }
    //         )
    //     }).collect();
    //
    //     println!("Execute + Get: {:?}", now.elapsed().as_millis());
    //     try_shutdown();
    // }
    //
    // #[test]
    // fn test_put_get_nested_remote() {
    //     try_init();
    //     const VEC_SIZE: usize = 1 << 12;
    //     let num_jobs = 1 << 0;
    //     let a: Vec<_> = (0u64..VEC_SIZE as u64).collect();
    //
    //     let now = std::time::Instant::now();
    //     let mut ids: Vec<_> = (0..num_jobs).map(|_| {
    //         put_and_get_nested.remote(&a)
    //     }).collect();
    //
    //     ids.reverse();
    //     println!("Submission: {:?}", now.elapsed().as_millis());
    //
    //     std::thread::sleep(std::time::Duration::from_millis(1000));
    //
    //     let results: Vec<_> = (0..num_jobs).map(|_| {
    //         let res = get::<Vec<u64>>(ids.pop().unwrap());
    //         assert_eq!(a, res);
    //         res
    //     }).collect();
    //
    //     println!("Execute + Get: {:?}", now.elapsed().as_millis());
    //     try_shutdown();
    // }
    //
    // #[test]
    // fn test_put_and_get() {
    //     try_init();
    //     const VEC_SIZE: usize = 1 << 12;
    //     let num_jobs = 1 << 0;
    //     let a: Vec<_> = (0u64..VEC_SIZE as u64).collect();
    //
    //     let now = std::time::Instant::now();
    //     let mut ids: Vec<_> = (0..num_jobs).map(|_| {
    //         put::<Vec<u64>, _>(&a)
    //     }).collect();
    //
    //     ids.reverse();
    //     println!("Submission: {:?}", now.elapsed().as_millis());
    //
    //     std::thread::sleep(std::time::Duration::from_millis(1000));
    //
    //     let results: Vec<_> = (0..num_jobs).map(|_| {
    //         let res = get::<Vec<u64>>(ids.pop().unwrap());
    //         assert_eq!(a, res);
    //         res
    //     }).collect();
    //
    //     println!("Execute + Get: {:?}", now.elapsed().as_millis());
    //     try_shutdown();
    // }

    // TODO: rework this
    // #[test]
    // fn test_get_execute_result() {
    //     try_init();
    //     let (a, b): (Vec<_>, Vec<_>) =
    //         ((0u64..100).collect(), (0u64..100).collect());
    //     let a_ser = rmp_serde::to_vec(&a).unwrap();
    //     let b_ser = rmp_serde::to_vec(&b).unwrap();
    //
    //     // let_cxx_string!(fn_name = "ray_rs_sys::remote_functions::ray_rust_ffi_add_two_vecs");
    //     let_cxx_string!(fn_name = "ray_rust_ffi_add_two_vecs");
    //     let ret_ffi: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
    //         &rust_worker_execute(
    //             vec![a_ser.as_ptr() as u64, b_ser.as_ptr() as u64],
    //             vec![a_ser.len() as u64, b_ser.len() as u64],
    //             &fn_name,
    //         )
    //     ).unwrap();
    //     assert_eq!(ret_ffi, (0u64..200).step_by(2).collect::<Vec<u64>>());
    //     try_shutdown();
    // }

    #[test]
    fn test_remote_function_wrapper_macro() {
        let (a, b, c): (Vec<_>, Vec<_>, Vec<_>) = (
            (0u64..100).collect(),
            (0u64..100).collect(),
            (0u64..100).collect(),
        );
        let a_ser = rmp_serde::to_vec(&a).unwrap();
        let b_ser = rmp_serde::to_vec(&b).unwrap();
        let c_ser = rmp_serde::to_vec(&c).unwrap();

        let args_ptrs = RustBuffer::from_vec(
            rmp_serde::to_vec(&(
                vec![a_ser.as_ptr() as u64, b_ser.as_ptr() as u64],
                vec![a_ser.len() as u64, b_ser.len() as u64],
            ))
            .unwrap(),
        );

        let ret: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &add_two_vecs.ray_call(args_ptrs).destroy_into_vec(),
        )
        .unwrap();
        assert_eq!(ret, (0u64..200).step_by(2).collect::<Vec<u64>>());

        let args_ptrs3 = RustBuffer::from_vec(
            rmp_serde::to_vec(&(
                vec![
                    a_ser.as_ptr() as u64,
                    b_ser.as_ptr() as u64,
                    c_ser.as_ptr() as u64,
                ],
                vec![a_ser.len() as u64, b_ser.len() as u64, c_ser.len() as u64],
            ))
            .unwrap(),
        );

        let ret3: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &add_three_vecs.ray_call(args_ptrs3).destroy_into_vec(),
        )
        .unwrap();
        assert_eq!(ret3, (0u64..300).step_by(3).collect::<Vec<u64>>());

        // Can only be called on nightly as fn traits are an unstable feature.
        #[cfg(nightly)]
        println!("{:?}", add_two_vecs(a.clone(), b.clone()));
        #[cfg(not(nightly))]
        println!("{:?}", add_two_vecs.call(a.clone(), b.clone()));

        println!("{:?}", add_two_vecs.name());
        println!("{:?}", add_three_vecs.name());
    }
}
