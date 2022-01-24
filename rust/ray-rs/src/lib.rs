#![cfg_attr(nightly, feature(fn_traits, unboxed_closures, min_specialization))]

#[macro_use]
pub use paste::paste;
#[macro_use]
pub use lazy_static::lazy_static;
pub use ctor::ctor;
pub use rmp_serde;
pub use uniffi::ffi::RustBuffer;
use core::pin::Pin;

pub use ray_rs_sys::*;
pub use ray_rs_sys::ray;

pub mod remote_functions;
pub use remote_functions::*;

pub use std::ffi::CString;

use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, Arc}, clone::Clone, os::raw::c_char,
    ops::{Deref, Drop}, marker::PhantomData,
    mem::drop,
};

use libloading::{Library, Symbol};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;

#[macro_export]
macro_rules! ray_info {
    ($($arg:tt)*) => {
        util::log_internal(format!("[rust] {}:{}: {}", file!(), line!(), format!($($arg)*)));
    }
}

lazy_static::lazy_static! {
    pub static ref GLOBAL_FUNCTION_NAMES_SET: Mutex<HashSet<CString>> = {
        Mutex::new(HashSet::new())
    };
}

lazy_static::lazy_static! {
    static ref GLOBAL_FUNCTION_MAP: Mutex<HashMap<CString, Symbol<'static, InvokerFunction>>> = {
        Mutex::new(HashMap::new())
    };
}

lazy_static::lazy_static! {
    static ref LIBRARIES: Mutex<Vec<Library>> = {
        Mutex::new(Vec::new())
    };
}

// Prints each argument on a separate line
//
// TODO: implement non-raw version
pub fn load_code_paths_from_raw_c_cmdline(argc: i32, argv: *mut *mut c_char) {
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

// ObjectRef ought to be thread-safe (check this)
// An ObjectRef has special treament by the RaySerializer

pub struct ObjectRef<T>(Arc<ObjectRefInner<T>>);

struct ObjectRefInner<T> {
    id: ObjectID,
    _phantom_data: PhantomData<T>,
}

impl<T> ObjectRef<T> {
    fn new(id: ObjectID) -> Self {
        util::add_local_ref(&id);
        Self(
            Arc::new(
                ObjectRefInner::<T> {
                    id: id,
                    _phantom_data: PhantomData,
                }
            )
        )
    }

    fn as_raw(&self) -> &ObjectID {
        &self.0.as_ref().id
    }
}

impl<T> Drop for ObjectRefInner<T> {
    fn drop(&mut self) {
        util::remove_local_ref(&self.id);
    }
}

// Prints each argument on a separate line
// pub fn load_code_paths_from_rust_cmdline() {
//     let slice = unsafe { std::slice::from_raw_parts(argv, argc as usize) };
//
//     for ptr in slice {
//         let arg = unsafe { std::ffi::CStr::from_ptr(*ptr).to_str().unwrap() };
//         if arg.starts_with("--ray_code_search_path=") {
//             let (_, path_str) = arg.clone().split_at("--ray_code_search_path=".len());
//             let paths = path_str.split(":").collect();
//             load_libraries_from_paths(&paths);
//         }
//     }
// }

// For safety to hold, the libraries cannot be unloaded once loaded
// Below, we are doing the incredibly unsafe "std::mem::transmute"
// to have the library lifetime be static
pub fn load_libraries_from_paths(paths: &Vec<&str>) {
    let mut libs = LIBRARIES.lock().unwrap();
    for path in paths {
        match unsafe { Library::new(path).ok() } {
            Some(lib) => {
                load_function_ptrs_from_library(&lib);
                libs.push(lib)
            },
            None => panic!("Shared-object library not found at path: {}", path),
        }
    }
}

fn load_function_ptrs_from_library(lib: &Library) {
    let mut fn_map = GLOBAL_FUNCTION_MAP.lock().unwrap();
    for fn_name in GLOBAL_FUNCTION_NAMES_SET.lock().unwrap().iter() {
        let fn_str = fn_name.to_str().unwrap();
        let ret = unsafe {
                lib.get::<InvokerFunction>(fn_str.as_bytes()).ok()
        };
        if let Some(symbol) = ret {
            ray_info!("Loaded function {} as {:?}", fn_str, symbol);
            let static_symbol = unsafe {
                std::mem::transmute::<Symbol<_, >, Symbol<'static, InvokerFunction>>(symbol)
            };
            fn_map.insert(fn_name.clone(), static_symbol);
        }
    }
}

pub extern "C" fn rust_worker_execute(
    _task_type: RayInt,
    ray_function_info: RaySlice,
    args: *const *const DataValue,
    args_len: u64,
    return_values: RaySlice,
) {
    // TODO (jon-chuang): One should replace RustBuffer with RaySlice...
    // TODO (jon-chuang): Try to move unsafe into ray_rs_sys
    // Replace all size_t with u64?
    let args_slice = unsafe {
        std::slice::from_raw_parts(
            args,
            args_len as usize,
        )
    };

    let mut arg_ptrs = Vec::<u64>::new();
    let mut arg_sizes = Vec::<u64>::new();

    for &arg in args_slice {
        // Todo: change this to ingest the raw ptr to hide unsafe
        // ray_rs_sys::util::dv_as_slice(*arg);
        unsafe {
            arg_ptrs.push((*(*arg).data).p as u64);
            arg_sizes.push((*(*arg).data).size as u64);
        }
    }

    let args_buffer = RustBuffer::from_vec(rmp_serde::to_vec(&(&arg_ptrs, &arg_sizes)).unwrap());
    // Since the string data was passed from the outer invocation context,
    // it will be destructed by that context with a lifetime that outlives this function body.
    let fn_name = std::mem::ManuallyDrop::new(
        unsafe {
            CString::from_raw(*(ray_function_info.data as *mut *mut std::os::raw::c_char))
        }
    );
    let fn_str = fn_name.to_str().unwrap();
    // Check if we get a cache hit
    let libs = LIBRARIES.lock().unwrap();
    let mut fn_map = GLOBAL_FUNCTION_MAP.lock().unwrap();

    let mut ret_ref = fn_map.get(fn_name.deref());

    // TODO(jon-chuang): figure out if you can narrow search
    // by mapping library name to function crate name...
    if let None = ret_ref {
        for lib in libs.iter() {
            let ret = unsafe {
                    lib.get::<InvokerFunction>(fn_str.as_bytes()).ok()
            };
            if let Some(symbol) = ret {
                ray_info!("Loaded function {} as {:?}", fn_str, symbol);
                let static_symbol = unsafe {
                    std::mem::transmute::<Symbol<_, >, Symbol<'static, InvokerFunction>>(symbol)
                };
                fn_map.insert(fn_name.deref().clone(), static_symbol);
                ret_ref = fn_map.get(fn_name.deref());
            }
        }
    } else {
        ray_info!("Using cached library symbol for {}: {:?}", fn_str, ret_ref);
    }
    let func = ret_ref.expect(&format!("Could not find symbol for fn of name {}", fn_str));
    ray_info!("Executing: {}", fn_str);
    let ret = func(args_buffer);
    ray_info!("Executed: {}", fn_str);

    let ret_owned = std::mem::ManuallyDrop::new(ret.destroy_into_vec());

    unsafe {
        let ret_slice = std::slice::from_raw_parts_mut(
            return_values.data as *mut *const DataValue,
            return_values.len as usize,
        );
        ret_slice[0] = c_worker_AllocateDataValue(
            ret_owned.as_ptr(),
            ret_owned.len() as u64,
            std::ptr::null(),
            0,
        );
    }
}
