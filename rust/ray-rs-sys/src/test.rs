#[cfg(test)]
mod test {
    use uniffi::RustBuffer;
    use rmp_serde;
    use ray_rs_sys::{ray_api_ffi::*, RustTaskArg, remote, add_two_vecs, add_three_vecs};
    use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

    #[test]
    fn test_remote_function_wrapper_macro() {
        let (a, b, c): (Vec<_>, Vec<_>, Vec<_>) =
            ((0u64..100).collect(), (0u64..100).collect(), (0u64..100).collect());
        let a_ser = rmp_serde::to_vec(&a).unwrap();
        let b_ser = rmp_serde::to_vec(&b).unwrap();
        let c_ser = rmp_serde::to_vec(&c).unwrap();

        let args_ptrs = RustBuffer::from_vec(rmp_serde::to_vec(
            &vec![
                (a_ser.as_ptr() as u64, b_ser.len()),
                (b_ser.as_ptr() as u64, b_ser.len()),
            ]
        ).unwrap());

        let ret: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &add_two_vecs.ray_call(args_ptrs).destroy_into_vec()
        ).unwrap();
        assert_eq!(ret, (0u64..200).step_by(2).collect::<Vec<u64>>());

        let args_ptrs3 = RustBuffer::from_vec(rmp_serde::to_vec(
            &vec![
                (a_ser.as_ptr() as u64, b_ser.len()),
                (b_ser.as_ptr() as u64, b_ser.len()),
                (c_ser.as_ptr() as u64, c_ser.len()),
            ]
        ).unwrap());

        let ret3: Vec<u64> = rmp_serde::from_read_ref::<_, Vec<u64>>(
            &add_three_vecs.ray_call(args_ptrs3).destroy_into_vec()
        ).unwrap();
        assert_eq!(ret3, (0u64..300).step_by(3).collect::<Vec<u64>>());

        // println!("{:?}", add_two_vecs(a.clone(), b.clone()));
        println!("{:?}", add_two_vecs.remote(a, b));

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
        // let_cxx_string!(name = "my_func");
        // Submit(&name, &Vec::new());
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
