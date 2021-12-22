#[cfg(test)]
mod test {
    use ray_rs_sys::{ray_api_ffi::*, RustTaskArg};
    use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

    #[test]
    fn test_rust() {

    }

    #[test]
    fn test_cpp_binding() {
        println!("{}", IsInitialized());
        Init();
        // InitAsLocal();
        println!("{}", IsInitialized());

        let_cxx_string!(name = "my_func");
        Submit(&name, &Vec::new());

        println!("\nPutting Uint64!");
        let x = PutUint64(1u64 << 20);
        println!("Uint64 ObjectID: {:x?}", x.ID().as_bytes());
        let int = *GetUint64(x);
        println!("Our integer: {}", int);

        println!("\nPutting string!");
        let_cxx_string!(string = "Rust is now on Ray!");
        let s = PutString(&string);
        println!("String data len: {}", string.len());
        println!("String ObjectID: {:x?}", s.ID().as_bytes());
        println!("Yipee! {}\n", *GetString(s));

        PutAndGetConfig();

        Shutdown();
        println!("{}", IsInitialized());
    }
}
