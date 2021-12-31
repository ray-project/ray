#![feature(fn_traits, unboxed_closures, min_specialization)]

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

use paste::paste;
use uniffi::ffi::RustBuffer;
use core::pin::Pin;
use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

mod remote_functions;
pub use remote_functions::{add_two_vecs, add_three_vecs, add_two_vecs_nested, get};

pub struct RustTaskArg {
    buf: Vec<u8>,
    object_id: String,
}

impl RustTaskArg {
    pub fn is_value(&self) -> bool {
        !self.buf.is_empty()
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn object_ref(&self) -> &str {
        &self.object_id
    }
}


use std::{collections::HashMap, sync::Mutex};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;

type FunctionPtrMap = HashMap<Vec<u8>, InvokerFunction>;


lazy_static::lazy_static! {
    static ref GLOBAL_FUNCTION_MAP: Mutex<FunctionPtrMap> = {
        Mutex::new([
            (add_two_vecs.name().as_bytes().to_vec(), add_two_vecs.get_invoker()),
            (add_two_vecs_nested.name().as_bytes().to_vec(), add_two_vecs_nested.get_invoker())
        ].iter().cloned().collect::<HashMap<_,_>>())
    };

}

pub fn get_execute_result(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8> {
    let args_buffer = RustBuffer::from_vec(rmp_serde::to_vec(&(&args, &sizes)).unwrap());
    let ret = GLOBAL_FUNCTION_MAP
        .lock()
        .unwrap()
        .get(&fn_name.as_bytes().to_vec())
        .expect(&format!("Could not find symbol for fn of name {:?}", fn_name))(args_buffer);
    ret.destroy_into_vec()
}


// TODO (jon-chuang): ensure you know how to handle the lifetime of RustBuffer safely...
// Understand how to safely handle lifetimes for FFI more generally.

// Here, the RustBuffer ought to be passed by value (move semantics) and thus disappear
// from scope on the Rust side.

// We need to handle the case where the key is not found with an error;
// however, we could also check in advance via the initially loaded function name mapping

// If we were to apply the function for the user directly in the application code
// and return the buffer (as was suggested for the C++ API), we would probably also
// reduce the attack surface
#[no_mangle]
extern "C" fn get_function_ptr(key: RustBuffer) -> Option<InvokerFunction> {
    let key_as_vec = key.destroy_into_vec();
    GLOBAL_FUNCTION_MAP.lock().unwrap().get(&key_as_vec).cloned()
}


// struct FunctionManager {
//     cache: FunctionPtrMap,
//     libs:
// }

#[repr(C)]
struct BufferData {
    ptr_u64: u64,
    size: u64,
}

type BufferRawPartsVec = Vec<(u64, u64)>;
struct ArgsBuffer(BufferRawPartsVec);

// TODO: maybe this more legit way...
impl ArgsBuffer {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add_arg(&mut self, ptr: *const u8, size: usize) {
        self.0.push((ptr as u64, size as u64));
    }

    pub fn to_rust_buffer(self) -> RustBuffer {
        RustBuffer::from_vec(rmp_serde::to_vec(&self.0).unwrap())
    }
}

// unsafe fn push_arg_slice_from_raw_parts(vec: &mut ArgsVec<'a>, data: *const u8, size: usize) {
//     vec.push((data, size as u64));
// }


#[cxx::bridge(namespace = "ray")]
pub mod ray_api_ffi {
    extern "Rust" {
        type RustTaskArg;
        fn is_value(&self) -> bool;
        fn value(&self) -> &Vec<u8>;
        fn object_ref(&self) -> &str;
    }

    extern "Rust" {
        fn get_execute_result(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8>;
    }

    // extern "Rust" {
    //     unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8>;
    // }

    unsafe extern "C++" {
        include!("rust/ray-rs-sys/cpp/tasks.h");

        type ObjectID;

        // fn Binary(self: &ObjectID) -> &CxxString;

        fn Submit(name: &str, args: &Vec<RustTaskArg>) -> UniquePtr<ObjectID>;
        fn InitRust();
    }

    unsafe extern "C++" {
        include!("ray/api.h");
        include!("rust/ray-rs-sys/cpp/wrapper.h");

        fn GetRaw(id: UniquePtr<ObjectID>) -> Vec<u8>;

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
