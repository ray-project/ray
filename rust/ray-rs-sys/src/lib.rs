#![feature(fn_traits, unboxed_closures, min_specialization)]

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

use paste::paste;
use uniffi::ffi::RustBuffer;
use core::pin::Pin;
use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

pub mod remote_functions;
pub use remote_functions::{
    add_two_vecs,
    add_three_vecs,
    add_two_vecs_nested,
    add_two_vecs_nested_remote_outer_get,
    get, put,
    put_and_get_nested
};

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


use std::{collections::HashMap, sync::Mutex, os::raw::c_char};

use libloading::{Library, Symbol};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;

type FunctionPtrMap = HashMap<Vec<u8>, Symbol<'static, InvokerFunction>>;


lazy_static::lazy_static! {
    static ref GLOBAL_FUNCTION_MAP: Mutex<FunctionPtrMap> = {
        Mutex::new([
            // (add_two_vecs.name().as_bytes().to_vec(), add_two_vecs.get_invoker()),
            // (add_two_vecs_nested.name().as_bytes().to_vec(), add_two_vecs_nested.get_invoker())
        ].iter().cloned().collect::<HashMap<_,_>>())
    };
}

lazy_static::lazy_static! {
    static ref LIBRARIES: Mutex<Vec<Library>> = {
        Mutex::new(Vec::new())
    };
}

// Prints each argument on a separate line
pub fn load_code_paths_from_cmdline(argc: i32, argv: *mut *mut c_char) {
    let slice = unsafe { std::slice::from_raw_parts(argv, argc as usize) };

    for ptr in slice {
        let arg = unsafe { std::ffi::CStr::from_ptr(*ptr).to_str().unwrap() };
        if arg.starts_with("--ray_code_search_path=") {
            let (_, path_str) = arg.clone().split_at("--ray_code_search_path=".len());
            let paths = path_str.split(":").collect();
            load_libraries_from_paths(&paths);
        }
    }
}

// For safety to hold, the libraries cannot be unloaded once loaded
// Below, we are doing the incredibly unsafe "std::mem::transmute"
// to have the library lifetime be static
pub fn load_libraries_from_paths(paths: &Vec<&str>) {
    let mut libs = LIBRARIES.lock().unwrap();
    for path in paths {
        match unsafe { Library::new(path).ok() } {
            Some(lib) => libs.push(lib),
            None => panic!("Shared-object library not found at path: {}", path),
        }
    }
}

pub fn get_execute_result(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8> {
    let args_buffer = RustBuffer::from_vec(rmp_serde::to_vec(&(&args, &sizes)).unwrap());
    // Check if we get a cache hit
    let libs = LIBRARIES.lock().unwrap();

    let mut fn_map = GLOBAL_FUNCTION_MAP.lock().unwrap();

    let mut ret_ref = fn_map.get(&fn_name.as_bytes().to_vec());
    // Check if we can get fn from available libraries

    // TODO(jon-chuang): figure out if you can narrow search
    // by mapping library name to function crate name...
    if let None = ret_ref {
        for lib in libs.iter() {
            let ret = unsafe {
                    lib.get::<InvokerFunction>(fn_name.to_str().unwrap().as_bytes()).ok()
            };
            ray_api_ffi::LogInfo(&format!("Loaded function {:?} as {:?}", fn_name.to_str().unwrap(), ret));
            if let Some(symbol) = ret {
                let static_symbol = unsafe {
                    std::mem::transmute::<Symbol<_, >, Symbol<'static, InvokerFunction>>(symbol)
                };
                fn_map.insert(fn_name.as_bytes().to_vec(), static_symbol);
                ret_ref = fn_map.get(&fn_name.as_bytes().to_vec());
            }
        }
    } else {
        ray_api_ffi::LogInfo(&format!("Using cached library symbol for {:?}: {:?}", fn_name.to_str().unwrap(), ret_ref));
    }
    let ret = ret_ref.expect(&format!("Could not find symbol for fn of name {:?}", fn_name))(args_buffer);
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
// #[no_mangle]
// extern "C" fn get_function_ptr(key: RustBuffer) -> Option<InvokerFunction> {
//     let key_as_vec = key.destroy_into_vec();
//     GLOBAL_FUNCTION_MAP.lock().unwrap().get(&key_as_vec).cloned()
// }


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

#[no_mangle]
fn initialize_core_worker(existing: SharedPtr<ray_api_ffi::CoreWorkerProcessImpl>) {
    ray_api_ffi::InitializeFromExisting(existing);
}

fn initialize_library_core_workers() {
    let libs = LIBRARIES.lock().unwrap();
    for lib in libs.iter() {
        let maybe_initializer = unsafe { lib.get::<fn(SharedPtr<ray_api_ffi::CoreWorkerProcessImpl>)->()>(b"initialize_core_worker") };
        maybe_initializer.expect("Could not locate initialize_core_worker in shared library. Try including the right macros in your Ray Rust project")(
            ray_api_ffi::GetCoreWorkerProcess()
        );
    }
}

fn initialize_library_core_workers_from_outer(outer: SharedPtr<ray_api_ffi::CoreWorkerProcessImpl>) {
    let libs = LIBRARIES.lock().unwrap();
    for lib in libs.iter() {
        let maybe_initializer = unsafe { lib.get::<fn(SharedPtr<ray_api_ffi::CoreWorkerProcessImpl>)->()>(b"initialize_core_worker") };
        maybe_initializer.expect("Could not locate initialize_core_worker in shared library. Try including the right macros in your Ray Rust project")(
            outer.clone()
        );
    }
}

fn object_id_to_byte_vec(id: UniquePtr<ray_api_ffi::ObjectID>) -> Vec<u8> {
    ray_api_ffi::ObjectIDString(id).as_bytes().to_vec()
}

pub fn byte_vec_to_object_id(vec: Vec<u8>) -> UniquePtr<ray_api_ffi::ObjectID> {
    let_cxx_string!(string = &vec);
    ray_api_ffi::StringObjectID(&string)
}

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
        unsafe fn load_code_paths_from_cmdline(argc: i32, argv: *mut *mut c_char);
        fn initialize_library_core_workers();
        fn initialize_library_core_workers_from_outer(outer: SharedPtr<CoreWorkerProcessImpl>);
    }

    // extern "Rust" {
    //     unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8>;
    // }

    unsafe extern "C++" {
        include!("rust/ray-rs-sys/cpp/tasks.h");

        type ObjectID;
        fn ObjectIDString(id: UniquePtr<ObjectID>) -> UniquePtr<CxxString>;
        fn StringObjectID(string: &CxxString) -> UniquePtr<ObjectID>;

        fn Submit(name: &str, args: &Vec<RustTaskArg>) -> UniquePtr<ObjectID>;
        fn InitRust(arg_str: &str);
    }

    unsafe extern "C++" {
        include!("rust/ray-rs-sys/cpp/api.h");
        include!("rust/ray-rs-sys/cpp/wrapper.h");

        fn LogDebug(str: &str);
        fn LogInfo(str: &str);

        fn GetRaw(id: UniquePtr<ObjectID>) -> Vec<u8>;
        fn PutRaw(data: Vec<u8>) -> UniquePtr<ObjectID>;
        //
        // type Uint64ObjectRef;
        // type StringObjectRef;
        //
        // fn InitAsLocal();
        // fn Init();
        fn IsInitialized() -> bool;
        fn Shutdown();
        //
        // fn PutUint64(obj: u64) -> UniquePtr<Uint64ObjectRef>;
        // fn GetUint64(obj_ref: UniquePtr<Uint64ObjectRef>) -> SharedPtr<u64>;
        //
        // fn PutString(obj: &CxxString) -> UniquePtr<StringObjectRef>;
        // fn GetString(obj_ref: UniquePtr<StringObjectRef>) -> SharedPtr<CxxString>;
        //
        // fn PutAndGetConfig();

        // fn ID(self: &Uint64ObjectRef) -> &CxxString;
        // fn ID(self: &StringObjectRef) -> &CxxString;
    }

    #[namespace = "ray::core"]
    unsafe extern "C++" {
        include!("ray/core_worker/core_worker_process.h");
        type CoreWorkerProcessImpl;

        pub fn InitializeFromExisting(existing_worker_process: SharedPtr<CoreWorkerProcessImpl>);
        pub fn GetCoreWorkerProcess() -> SharedPtr<CoreWorkerProcessImpl>;
    }
}
