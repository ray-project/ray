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

unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8> {
    let mut vec = Vec::with_capacity(size);
    let slice = core::slice::from_raw_parts(data, size);
    vec.copy_from_slice(slice);
    vec
}

use std::{collections::HashMap, sync::Mutex};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;

type FunctionPtrMap = HashMap<Vec<u8>, InvokerFunction>;

lazy_static::lazy_static! {
    static ref GLOBAL_FUNCTION_MAP: Mutex<FunctionPtrMap> =
        Mutex::new(FunctionPtrMap::new());
}

// TODO: ensure you know how to handle the lifetime of RustBuffer safely...
// Understand how to safely handle lifetimes for FFI more generally.
#[no_mangle]
extern "C" fn get_function_ptr(key: RustBuffer) -> Option<InvokerFunction> {
    let key_as_vec = key.destroy_into_vec();
    GLOBAL_FUNCTION_MAP.lock().unwrap().get(&key_as_vec).cloned()
}

#[cxx::bridge(namespace = "ray")]
pub mod ray_api_ffi {
    extern "Rust" {
        type RustTaskArg;
        fn is_value(&self) -> bool;
        fn value(&self) -> &Vec<u8>;
        fn object_ref(&self) -> &UniquePtr<CxxString>;
    }

    extern "Rust" {
        unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8>;
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
