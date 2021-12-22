#[cxx::bridge(namespace = "ray::core")]
mod ray_ffi {
    unsafe extern "C++" {
        include!("ray/core_worker/core_worker.h");
        include!("ray/core_worker/core_worker_options.h");
        include!("ray/common/id.h");

        type CoreWorker;
        type CoreWorkerOptions;
        #[namespace = "ray"]
        type WorkerID;

        fn ConnectToRaylet(self: Pin<&mut CoreWorker>);
        fn Shutdown(self: Pin<&mut CoreWorker>);
        fn GetWorkerID(self: &CoreWorker) -> &WorkerID;
    }
}

use uniffi::ffi::RustBuffer;
use core::pin::Pin;
use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

pub struct RustTaskArg {
    buf: Vec<u8>,
    object_id: Pin<Box<UniquePtr<CxxString>>>,
}

impl RustTaskArg {
    pub fn is_value(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn object_ref(&self) -> &UniquePtr<CxxString> {
        &(*self.object_id)
    }
}

#[cxx::bridge(namespace = "ray")]
pub mod ray_api_ffi {
    extern "Rust" {
        type RustTaskArg;
        fn is_value(&self) -> bool;
        fn value(&self) -> &Vec<u8>;
        fn object_ref(&self) -> &UniquePtr<CxxString>;
    }

    unsafe extern "C++" {
        include!("rust/ray-rs-sys/cpp/tasks.h");

        type ObjectID;

        fn Submit(name: &CxxString, args: &Vec<RustTaskArg>) -> UniquePtr<ObjectID>;
        fn InitRust();
    }

    unsafe extern "C++" {
        include!("ray/api.h");
        include!("rust/ray-rs-sys/cpp/wrapper.h");

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

        fn PutAndGetConfig();

        fn ID(self: &Uint64ObjectRef) -> &CxxString;
        fn ID(self: &StringObjectRef) -> &CxxString;
    }
}

// struct ObjectRef<T> {
//     _type: PhantomData<T>
// }
