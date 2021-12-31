#[cfg(test)]
mod test {
    use uniffi::RustBuffer;
    use rmp_serde;
    use ray_rs_sys::{ray_api_ffi::*, RustTaskArg, remote, add_two_vecs, add_two_vecs_nested, add_three_vecs, get, get_execute_result};
    use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

    // #[test]
    // fn test_init_submit_execute_shutdown() {
    //     const VEC_SIZE: usize = 1 << 12;
    //     let num_jobs = 1 << 0;
    //
    //     InitRust();
    //
    //     let now = std::time::Instant::now();
    //     let mut ids: Vec<_> = (0..num_jobs).map(|_| {
    //         let (a, b): (Vec<_>, Vec<_>) =
    //             ((0u64..VEC_SIZE as u64).collect(), (0u64..VEC_SIZE as u64).collect());
    //         add_two_vecs.remote(a, b)
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
    //
    //     Shutdown();
    // }

    #[test]
    fn test_nested_remote() {
        const VEC_SIZE: usize = 1 << 12;
        let num_jobs = 1 << 0;

        InitRust();

        let now = std::time::Instant::now();
        let mut ids: Vec<_> = (0..num_jobs).map(|_| {
            let (a, b): (Vec<_>, Vec<_>) =
                ((0u64..VEC_SIZE as u64).collect(), (0u64..VEC_SIZE as u64).collect());
            add_two_vecs_nested.remote(a, b)
        }).collect();

        ids.reverse();
        println!("Submission: {:?}", now.elapsed().as_millis());

        let results: Vec<_> = (0..num_jobs).map(|_| {
            get::<Vec<u64>>(ids.pop().unwrap())
        }).collect();

        println!("Execute + Get: {:?}", now.elapsed().as_millis());

        Shutdown();
    }


    #[test]
    fn test_get_execute_result() {
        let (a, b): (Vec<_>, Vec<_>) =
            ((0u64..100).collect(), (0u64..100).collect());
        let a_ser = rmp_serde::to_vec(&a).unwrap();
        let b_ser = rmp_serde::to_vec(&b).unwrap();

        let_cxx_string!(fn_name = "ray_rs_sys::remote_functions::ray_rust_ffi_add_two_vecs");
        let ret_ffi: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &get_execute_result(
                vec![a_ser.as_ptr() as u64, b_ser.as_ptr() as u64],
                vec![a_ser.len() as u64, b_ser.len() as u64],
                &fn_name,
            )
        ).unwrap();
        assert_eq!(ret_ffi, (0u64..200).step_by(2).collect::<Vec<u64>>());
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
