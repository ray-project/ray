#[cfg(test)]
mod test {
    use uniffi::RustBuffer;
    use rmp_serde;
    use ray_rs_sys::{ray_api_ffi::*,
        RustTaskArg, remote,
        add_two_vecs, add_two_vecs_nested_remote_outer_get,
        add_two_vecs_nested,
        add_three_vecs,
        put_and_get_nested,
        get, get_execute_result, load_libraries_from_paths,
        byte_vec_to_object_id,
    };
    use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};
    use std::sync::Mutex;
    use lazy_static::lazy_static;

    const NUM_CLUSTER_TESTS: usize = 2;

    lazy_static! {
        static ref CLUSTER_TEST_COUNTER: Mutex<(usize, usize)> = Mutex::new((0, 0));
    }

    fn try_init() {
        let mut guard = CLUSTER_TEST_COUNTER.lock().unwrap();
        if guard.0 == 0 {
            let arg = std::env::var("RAY_RUST_LIBRARY_PATHS").unwrap();
            if arg.starts_with("--ray_code_search_path=") {
                let (_, path_str) = arg.split_at("--ray_code_search_path=".len());
                let paths = path_str.split(":").collect();
                println!("{:?}", paths);
                load_libraries_from_paths(&paths);
            }
            InitRust(&arg);
        }
        guard.0 += 1;
    }

    fn try_shutdown() {
        let mut guard = CLUSTER_TEST_COUNTER.lock().unwrap();
        guard.1 += 1;
        if guard.1 == NUM_CLUSTER_TESTS {
            Shutdown();
            println!("Shutting down. Status: running = {}", IsInitialized());
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

    // #[test]
    // fn test_nested_remote() {
    //     try_init();
    //     const VEC_SIZE: usize = 1 << 12;
    //     let num_jobs = 1 << 0;
    //     let (a, b): (Vec<_>, Vec<_>) =
    //         ((0u64..VEC_SIZE as u64).collect(), (0u64..VEC_SIZE as u64).collect());
    //
    //     let now = std::time::Instant::now();
    //     let mut ids: Vec<_> = (0..num_jobs).map(|_| {
    //         add_two_vecs_nested.remote(&a, &b)
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
    //
    // #[test]
    // fn test_nested_remote_outer_get() {
    //     try_init();
    //     const VEC_SIZE: usize = 1 << 12;
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
    //         get::<Vec<u64>>(byte_vec_to_object_id(byte_vec))
    //     }).collect();
    //
    //     println!("Execute + Get: {:?}", now.elapsed().as_millis());
    //     try_shutdown();
    // }

    #[test]
    fn test_put_get_nested_remote() {
        try_init();
        const VEC_SIZE: usize = 1 << 12;
        let num_jobs = 1 << 0;
        let a: Vec<_> = (0u64..VEC_SIZE as u64).collect();

        let now = std::time::Instant::now();
        let mut ids: Vec<_> = (0..num_jobs).map(|_| {
            put_and_get_nested.remote(&a)
        }).collect();

        ids.reverse();
        println!("Submission: {:?}", now.elapsed().as_millis());

        std::thread::sleep(std::time::Duration::from_millis(1000));

        let results: Vec<_> = (0..num_jobs).map(|_| {
            let res = get::<Vec<u64>>(ids.pop().unwrap());
            assert_eq!(a, res);
            res
        }).collect();

        println!("Execute + Get: {:?}", now.elapsed().as_millis());
        try_shutdown();
    }


    #[test]
    fn test_get_execute_result() {
        try_init();
        let (a, b): (Vec<_>, Vec<_>) =
            ((0u64..100).collect(), (0u64..100).collect());
        let a_ser = rmp_serde::to_vec(&a).unwrap();
        let b_ser = rmp_serde::to_vec(&b).unwrap();

        // let_cxx_string!(fn_name = "ray_rs_sys::remote_functions::ray_rust_ffi_add_two_vecs");
        let_cxx_string!(fn_name = "ray_rust_ffi_add_two_vecs");
        let ret_ffi: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &get_execute_result(
                vec![a_ser.as_ptr() as u64, b_ser.as_ptr() as u64],
                vec![a_ser.len() as u64, b_ser.len() as u64],
                &fn_name,
            )
        ).unwrap();
        assert_eq!(ret_ffi, (0u64..200).step_by(2).collect::<Vec<u64>>());
        try_shutdown();
    }

    #[test]
    fn test_remote_function_wrapper_macro() {
        let (a, b, c): (Vec<_>, Vec<_>, Vec<_>) =
            ((0u64..100).collect(), (0u64..100).collect(), (0u64..100).collect());
        let a_ser = rmp_serde::to_vec(&a).unwrap();
        let b_ser = rmp_serde::to_vec(&b).unwrap();
        let c_ser = rmp_serde::to_vec(&c).unwrap();

        let args_ptrs = RustBuffer::from_vec(rmp_serde::to_vec(
            &(
                vec![a_ser.as_ptr() as u64, b_ser.as_ptr() as u64],
                vec![a_ser.len() as u64, b_ser.len() as u64],
            )
        ).unwrap());

        let ret: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &add_two_vecs.ray_call(args_ptrs).destroy_into_vec()
        ).unwrap();
        assert_eq!(ret, (0u64..200).step_by(2).collect::<Vec<u64>>());

        let args_ptrs3 = RustBuffer::from_vec(rmp_serde::to_vec(
            &(
                vec![a_ser.as_ptr() as u64, b_ser.as_ptr() as u64, c_ser.as_ptr() as u64],
                vec![a_ser.len() as u64, b_ser.len() as u64, c_ser.len() as u64],
            )
        ).unwrap());

        let ret3: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &add_three_vecs.ray_call(args_ptrs3).destroy_into_vec()
        ).unwrap();
        assert_eq!(ret3, (0u64..300).step_by(3).collect::<Vec<u64>>());

        println!("{:?}", add_two_vecs(a.clone(), b.clone()));
        // println!("{:?}", add_two_vecs.remote(a, b));

        println!("{:?}", add_two_vecs.name());
        println!("{:?}", add_three_vecs.name());
    }

    #[test]
    fn test_cpp_binding() {
        // println!("{}", IsInitialized());
        // Init();
        // // InitAsLocal();
        // println!("{}", IsInitialized());
        //
        // println!("\nPutting Uint64!");
        // let x = PutUint64(1u64 << 20);
        // println!("Uint64 ObjectID: {:x?}", x.ID().as_bytes());
        // let int = *GetUint64(x);
        // println!("Our integer: {}", int);
        //
        // println!("\nPutting string!");
        // let_cxx_string!(string = "Rust is now on Ray!");
        // let s = PutString(&string);
        // println!("String data len: {}", string.len());
        // println!("String ObjectID: {:x?}", s.ID().as_bytes());
        // println!("Yipee! {}\n", *GetString(s));
        //
        // PutAndGetConfig();
        //
        // Shutdown();
        // println!("{}", IsInitialized());
    }
}
