// #[cxx::bridge(namespace = "ray::core")]
// mod ray_ffi {
//     unsafe extern "C++" {
//         include!("ray/core_worker/core_worker.h");
//         include!("ray/core_worker/core_worker_options.h");
//         include!("ray/common/id.h");
//
//         type CoreWorker;
//         type CoreWorkerOptions;
//         #[namespace = "ray"]
//         type WorkerID;
//
//         fn ConnectToRaylet(self: Pin<&mut CoreWorker>);
//         fn Shutdown(self: Pin<&mut CoreWorker>);
//         fn GetWorkerID(self: &CoreWorker) -> &WorkerID;
//     }
// }

use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr};


#[cxx::bridge(namespace = "ray")]
mod ray_api_ffi {
    unsafe extern "C++" {
        include!("ray/api.h");
        include!("rust/include/wrapper.h");

        type Uint64ObjectRef;
        type StringObjectRef;

        fn InitAsLocal();
        fn Init();
        fn IsInitialized() -> bool;
        fn Shutdown();

        fn PutUint64(obj: u64) -> UniquePtr<Uint64ObjectRef>;
        fn GetUint64(obj_ref: UniquePtr<Uint64ObjectRef>) -> SharedPtr<u64>;

        fn PutString(obj: &CxxString) -> UniquePtr<StringObjectRef>;
        fn GetString(obj_ref: UniquePtr<StringObjectRef>) -> SharedPtr<CxxString>;

        fn ID(self: &Uint64ObjectRef) -> &CxxString;
        fn ID(self: &StringObjectRef) -> &CxxString;
    }
}

fn main() {
    use ray_api_ffi::*;

    println!("{}", IsInitialized());
    Init();
    // InitAsLocal();
    println!("{}", IsInitialized());

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

    Shutdown();
    println!("{}", IsInitialized());
}
