#![cfg_attr(nightly, feature(fn_traits, unboxed_closures, min_specialization))]

#[macro_use]
pub use paste::paste;
#[macro_use]
pub use lazy_static::lazy_static;
use core::pin::Pin;
pub use ctor::ctor;
pub use rmp_serde;
pub use uniffi::ffi::RustBuffer;

pub use ray_rs_sys::ray;
pub use ray_rs_sys::*;

pub mod remote_functions;
pub use remote_functions::*;

pub use std::ffi::CString;

use std::{
    clone::Clone,
    collections::{HashMap, HashSet},
    convert::TryInto,
    marker::PhantomData,
    mem::drop,
    ops::{Deref, Drop},
    os::raw::c_char,
    sync::{Arc, Mutex},
};

use libloading::{Library, Symbol};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;
type ActorMethod = extern "C" fn(*mut std::os::raw::c_void, RustBuffer) -> RustBuffer;
type ActorCreation = extern "C" fn(RustBuffer) -> *mut std::os::raw::c_void;

#[macro_export]
macro_rules! ray_info {
    ($($arg:tt)*) => {
        util::log_internal(format!("[rust] {}:{}: {}", file!(), line!(), format!($($arg)*)));
    }
}

lazy_static::lazy_static! {
    static ref LIBRARIES: Mutex<Vec<Library>> = {
        Mutex::new(Vec::new())
    };

    pub static ref GLOBAL_FUNCTION_NAMES_SET: Mutex<HashSet<CString>> = {
        Mutex::new(HashSet::new())
    };
    pub static ref GLOBAL_ACTOR_METHOD_NAMES_SET: Mutex<HashSet<CString>> = {
        Mutex::new(HashSet::new())
    };
    pub static ref GLOBAL_ACTOR_CREATION_NAMES_SET: Mutex<HashSet<CString>> = {
        Mutex::new(HashSet::new())
    };

    static ref GLOBAL_FUNCTION_MAP: Mutex<HashMap<CString, Symbol<'static, InvokerFunction>>> = {
        Mutex::new(HashMap::new())
    };
    static ref GLOBAL_ACTOR_METHODS_MAP: Mutex<HashMap<CString, Symbol<'static, ActorMethod>>> = {
        Mutex::new(HashMap::new())
    };
    static ref GLOBAL_ACTOR_CREATION_MAP: Mutex<HashMap<CString, Symbol<'static, ActorCreation>>> = {
        Mutex::new(HashMap::new())
    };
}
// Prints each argument on a separate line
//
// TODO: implement non-raw version
// TODO: rename load_libraries_*
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
        Self(Arc::new(ObjectRefInner::<T> {
            id: id,
            _phantom_data: PhantomData,
        }))
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
            }
            None => panic!("Shared-object library not found at path: {}", path),
        }
    }
}

macro_rules! load_names_to_ptrs {
    ($lib:ident, $map:ident, $names:ident, $sig:ty) => {
        let mut map = $map.lock().unwrap();
        for fn_name in $names.lock().unwrap().iter() {
            let fn_str = fn_name.to_str().unwrap();
            let ret = unsafe { $lib.get::<$sig>(fn_str.as_bytes()).ok() };
            if let Some(symbol) = ret {
                ray_info!("Pre-loaded function {} as {:?}", fn_str, symbol);
                let static_symbol =
                    unsafe { std::mem::transmute::<Symbol<_>, Symbol<'static, $sig>>(symbol) };
                map.insert(fn_name.clone(), static_symbol);
            }
        }
    };
}

fn load_function_ptrs_from_library(lib: &Library) {
    load_names_to_ptrs!(
        lib,
        GLOBAL_FUNCTION_MAP,
        GLOBAL_FUNCTION_NAMES_SET,
        InvokerFunction
    );
    load_names_to_ptrs!(
        lib,
        GLOBAL_ACTOR_CREATION_MAP,
        GLOBAL_ACTOR_CREATION_NAMES_SET,
        ActorCreation
    );
    load_names_to_ptrs!(
        lib,
        GLOBAL_ACTOR_METHODS_MAP,
        GLOBAL_ACTOR_METHOD_NAMES_SET,
        ActorMethod
    );
}

fn resolve_fn<'a, F>(
    fn_name: &CString,
    fn_str: &str,
    map: &'a mut HashMap<CString, Symbol<'static, F>>,
) -> &'a Symbol<'static, F> {
    use std::collections::hash_map::Entry;

    map.entry(fn_name.clone()).or_insert_with(|| {
        for lib in LIBRARIES.lock().unwrap().iter() {
            let ret = unsafe { lib.get::<F>(fn_str.as_bytes()).ok() };
            if let Some(symbol) = ret {
                ray_info!("Loaded function {} as {:?}", fn_str, symbol);
                let static_symbol =
                    unsafe { std::mem::transmute::<Symbol<_>, Symbol<'static, F>>(symbol) };
                return static_symbol;
            }
        }
        ray_info!(
            "{}",
            format!("Could not find symbol for fn of name {}", fn_str)
        );
        panic!();
    })
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Vec2 {
    x: u64,
    y: u64,
}

pub extern "C" fn rust_worker_execute(
    actor_ptr: *mut *mut std::os::raw::c_void,
    task_type_int: i32,
    ray_function_info: RaySlice,
    args: *const *const DataValue,
    args_len: u64,
    return_values: RaySlice,
) {
    let task_type = internal::parse_task_type(task_type_int);
    // TODO (jon-chuang): One should replace RustBuffer with RaySlice...
    // TODO (jon-chuang): Try to move unsafe into ray_rs_sys
    // Replace all size_t with u64?
    let args_slice = unsafe { std::slice::from_raw_parts(args, args_len as usize) };

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
    let fn_name = std::mem::ManuallyDrop::new(unsafe {
        CString::from_raw(*(ray_function_info.data as *mut *mut std::os::raw::c_char))
    });
    let fn_str = fn_name.to_str().unwrap();

    ray_info!(
        "Executing: {:?} with {} args (task name: {})",
        task_type,
        args_slice.len(),
        fn_str,
    );

    if task_type == ray::TaskType::ACTOR_CREATION_TASK {
        let mut fn_map = GLOBAL_ACTOR_CREATION_MAP.lock().unwrap();
        let a_ptr = resolve_fn(&fn_name, fn_str, &mut fn_map)(args_buffer);
        ray_info!("Actor Created @ address {:p} by {}", a_ptr, fn_str);
        unsafe {
            *actor_ptr = a_ptr;
        }
    } else {
        let ret = if task_type == ray::TaskType::NORMAL_TASK {
            let mut fn_map = GLOBAL_FUNCTION_MAP.lock().unwrap();
            let ret = resolve_fn(&fn_name, fn_str, &mut fn_map)(args_buffer);
            ray_info!("Executed: {}", fn_str);
            ret
        } else if task_type == ray::TaskType::ACTOR_TASK {
            let mut fn_map = GLOBAL_ACTOR_METHODS_MAP.lock().unwrap();
            // This will result in a sigsev if the actor has not been initialized
            let a_ptr = unsafe { *actor_ptr };
            let ret = resolve_fn(&fn_name, fn_str, &mut fn_map)(a_ptr, args_buffer);
            ray_info!("Executed {} on actor at {:p}", fn_str, a_ptr);
            ret
        } else {
            ray_info!("Invalid task type");
            panic!();
        };

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
}
