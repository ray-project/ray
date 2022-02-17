#![cfg_attr(nightly, feature(fn_traits, unboxed_closures, min_specialization))]

#[macro_use]
pub use paste::paste;
#[macro_use]
pub use lazy_static::lazy_static;
use core::pin::Pin;
pub use ctor::ctor;
pub use rmp_serde;
use uniffi::ffi::RustBuffer;
pub use async_ffi::{FfiFuture, FutureExt};

pub use ray_rs_sys::ray;
pub use ray_rs_sys::*;

pub mod remote_functions;
pub use remote_functions::*;

mod async_executor;
use async_executor::{ASYNC_RUNTIME_SENDER, handle_async_startup};
pub use async_executor::*;

pub use std::ffi::CString;

use std::{
    clone::Clone,
    cell::RefCell,
    collections::{HashMap, HashSet},
    convert::TryInto,
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, Drop},
    os::raw::c_char,
    sync::{Arc, Mutex, RwLock, atomic::{AtomicU64, Ordering}},
};

use libloading::{Library, Symbol};

type InvokerFunction = extern "C" fn(RayRustBuffer) -> RayRustBuffer;
type ActorMethod = extern "C" fn(ActorPtr, RayRustBuffer) -> RayRustBuffer;
type AsyncActorMethod = extern "C" fn(ActorPtr, RayRustBuffer) -> FfiFuture<RayRustBuffer>;
type ActorCreation = extern "C" fn(RayRustBuffer) -> *mut std::os::raw::c_void;

#[macro_export]
macro_rules! ray_info {
    ($($arg:tt)*) => {
        util::log_internal(format!("[rust] {}:{}: {}", file!(), line!(), format!($($arg)*)));
    }
}

lazy_static! {
    static ref LIBRARIES: RwLock<Vec<Library>> = {
        RwLock::new(Vec::new())
    };

    pub static ref GLOBAL_FUNCTION_NAMES_SET: RwLock<HashSet<CString>> = {
        RwLock::new(HashSet::new())
    };
    pub static ref GLOBAL_ACTOR_METHOD_NAMES_SET: RwLock<HashSet<CString>> = {
        RwLock::new(HashSet::new())
    };
    pub static ref GLOBAL_ASYNC_ACTOR_METHOD_NAMES_SET: RwLock<HashSet<CString>> = {
        RwLock::new(HashSet::new())
    };
    pub static ref GLOBAL_ACTOR_CREATION_NAMES_SET: RwLock<HashSet<CString>> = {
        RwLock::new(HashSet::new())
    };

    static ref GLOBAL_FUNCTION_MAP: RwLock<HashMap<CString, Symbol<'static, InvokerFunction>>> = {
        RwLock::new(HashMap::new())
    };
    static ref GLOBAL_ACTOR_METHODS_MAP: RwLock<HashMap<CString, Symbol<'static, ActorMethod>>> = {
        RwLock::new(HashMap::new())
    };
    static ref GLOBAL_ASYNC_ACTOR_METHODS_MAP: RwLock<HashMap<CString, Symbol<'static, AsyncActorMethod>>> = {
        RwLock::new(HashMap::new())
    };
    static ref GLOBAL_ACTOR_CREATION_MAP: RwLock<HashMap<CString, Symbol<'static, ActorCreation>>> = {
        RwLock::new(HashMap::new())
    };
}

static NUM_TASKS_EXECUTED: AtomicU64 = AtomicU64::new(0);

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

impl<T> std::fmt::Debug for ObjectRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.as_raw().fmt(f)
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
// to have the lifetimes of the symbols loaded be static
pub fn load_libraries_from_paths(paths: &Vec<&str>) {
    let mut libs = LIBRARIES.write().unwrap();
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
        let mut map = $map.write().unwrap();
        for fn_name in $names.read().unwrap().iter() {
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
    load_names_to_ptrs!(
        lib,
        GLOBAL_ASYNC_ACTOR_METHODS_MAP,
        GLOBAL_ASYNC_ACTOR_METHOD_NAMES_SET,
        AsyncActorMethod
    );
}

// TODO: change this to return `Result`
fn resolve_fn<F>(
    fn_name: &CString,
    fn_str: &str,
    fn_map: &RwLock<HashMap<CString, Symbol<'static, F>>>,
) -> Option<Symbol<'static, F>> {
    use std::collections::hash_map::Entry;

    let mut maybe_symbol_ref = fn_map.read().unwrap().get(fn_name).cloned();
    if let None = maybe_symbol_ref {
        for lib in LIBRARIES.read().unwrap().iter() {
            let ret = unsafe { lib.get::<F>(fn_str.as_bytes()).ok() };
            if let Some(symbol) = ret {
                ray_info!("Loaded function {} as {:?}", fn_str, symbol);
                let static_symbol = unsafe { std::mem::transmute::<Symbol<_>, Symbol<'static, F>>(symbol) };
                fn_map.write().unwrap().insert(fn_name.clone(), static_symbol.clone());
                maybe_symbol_ref = Some(static_symbol);
                break;
            }
        }
    }
    if let None = maybe_symbol_ref {
        ray_info!(
            "{}",
            format!("Could not find symbol for fn of name {}", fn_str)
        );
    }
    maybe_symbol_ref
}

// #[cfg(feature = "async")]
// fn handle_async_shutdown() {
//     let mut guard = ASYNC_RUNTIME.lock().unwrap();
//     match &mut *guard {
//         // TODO: do proper error handling here.
//         Some(rt) => {
//             // TODO: figure out appropriate wait time (or force by setting to 0?)
//             rt.shutdown_timeout(std::time::Duration::from_millis(2_000));
//             *guard = None;
//         }
//         _ => (),
//     };
// }

struct TaskData {
    actor_ptr: *mut *mut std::os::raw::c_void,
    task_type_int: i32,
    ray_function_info: RaySlice,
    args: *const *const DataValue,
    args_len: u64,
    return_values: RaySlice,
}

unsafe impl Send for TaskData {}
unsafe impl Sync for TaskData {}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ActorPtr {
    pub ptr: *mut std::os::raw::c_void,
}

unsafe impl Send for ActorPtr {}
unsafe impl Sync for ActorPtr {}

pub struct RayRustBuffer(RustBuffer);

impl RayRustBuffer {
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self(RustBuffer::from_vec(v))
    }
    pub fn destroy_into_vec(self) -> Vec<u8> {
        self.0.destroy_into_vec()
    }
}

unsafe impl Send for RayRustBuffer {}
unsafe impl Sync for RayRustBuffer {}

#[cfg(feature = "async")]
pub extern "C" fn rust_worker_execute(
    is_async: bool,
    actor_ptr: *mut *mut std::os::raw::c_void,
    task_type_int: i32,
    ray_function_info: RaySlice,
    args: *const *const DataValue,
    args_len: u64,
    return_values: RaySlice,
) {
    if is_async {
        let mut notifier = Arc::new(FiberEvent::new());
        let notifier_ = notifier.clone();

        handle_async_startup();

        ASYNC_RUNTIME_SENDER
            .lock()
            .unwrap()
            .as_ref()
            .expect("runtime is not initialized")
            .send(
                (TaskData {
                    actor_ptr,
                    task_type_int,
                    ray_function_info,
                    args,
                    args_len,
                    return_values,
                },
                notifier_)
            );

        // Not sure if this is safe. When we call yield, are we sure
        // boost.asio is aware enough to save all of Rust's (or other lang's)
        // live state?
        notifier.yield_and_await();
        eprintln!("Number of tasks executed: {}", NUM_TASKS_EXECUTED.fetch_add(1, Ordering::Relaxed));
    } else {
        rust_worker_execute_internal(
            actor_ptr,
            task_type_int,
            ray_function_info,
            args,
            args_len,
            return_values,
        );
    }
}


// TODO (jon-chuang): One should replace RayRustBuffer with RaySlice...
// TODO (jon-chuang): Try to move unsafe into ray_rs_sys
// Replace all size_t with u64?
fn prepare_inputs(
    task_type_int: i32,
    ray_function_info: RaySlice,
    args: *const *const DataValue,
    args_len: u64
) -> (RayRustBuffer, ManuallyDrop<CString>, ray::TaskType) {
    let args_slice = unsafe { std::slice::from_raw_parts(args, args_len as usize) };

    let mut arg_ptrs = Vec::<u64>::new();
    let mut arg_sizes = Vec::<u64>::new();

    for &arg in args_slice {
        // Todo: change this to ingest the raw ptr to hide unsafe
        unsafe {
            arg_ptrs.push((*(*arg).data).p as u64);
            arg_sizes.push((*(*arg).data).size as u64);
        }
    }

    let args_buffer = RayRustBuffer::from_vec(rmp_serde::to_vec(&(&arg_ptrs, &arg_sizes)).unwrap());
    // Since the string data was passed from the outer invocation context,
    // it will be destructed by that context with a lifetime that outlives this function body.
    let fn_name = std::mem::ManuallyDrop::new(unsafe {
        CString::from_raw(*(ray_function_info.data as *mut *mut std::os::raw::c_char))
    });

    let task_type = internal::parse_task_type(task_type_int);

    ray_info!(
        "Executing: {:?} with {} args (task name: {:?})",
        task_type,
        args_slice.len(),
        *fn_name,
    );

    (args_buffer, fn_name, task_type)
}

async fn rust_worker_execute_async_internal(task_data: TaskData) {
    let (args_buffer, fn_name, task_type) = prepare_inputs(
        task_data.task_type_int,
        task_data.ray_function_info,
        task_data.args,
        task_data.args_len
    );
    // Better error handling...?
    let fn_str = fn_name.to_str().unwrap();

    if task_type == ray::TaskType::ACTOR_CREATION_TASK {
        // TODO: handle the resolve failure properly...
        let a_ptr = resolve_fn(&fn_name, fn_str, &GLOBAL_ACTOR_CREATION_MAP).unwrap()(args_buffer);
        ray_info!("Actor Created @ address {:p} by {}", a_ptr, fn_str);
        // TODO: this needs to be wrapped with a thread-safe lock managed from the c_worker side
        // i.e. c_worker_AssignActorPtr(old, new); Then we can also get rid of **void
        unsafe {
            *task_data.actor_ptr = a_ptr;
        }
    } else {
        let ret = if task_type == ray::TaskType::NORMAL_TASK {
            let ret = resolve_fn(&fn_name, fn_str, &GLOBAL_FUNCTION_MAP).unwrap()(args_buffer);
            ray_info!("Executed: {}", fn_str);
            ret
        } else if task_type == ray::TaskType::ACTOR_TASK {
            // This will result in a sigsegv if the actor has not been initialized
            //
            // Also, we may want the runtimes to be able to move the actor ptr...
            // now that is a different story...
            // and we do indeed need **void to be passed to the user-side fn. But do
            // we really want that?
            //
            // So we may also want a "get actor ptr instead...".
            let a_ptr = ActorPtr { ptr: unsafe { *task_data.actor_ptr } };

            let ret = if fn_str.starts_with("ray_rust_async_") {
                resolve_fn(&fn_name, fn_str, &GLOBAL_ASYNC_ACTOR_METHODS_MAP).unwrap()(a_ptr, args_buffer).await
            } else {
                resolve_fn(&fn_name, fn_str, &GLOBAL_ACTOR_METHODS_MAP).unwrap()(a_ptr, args_buffer)
            };
            ray_info!("Executed {} on actor at {:?}", fn_str, a_ptr);
            ret
        } else {
            ray_info!("Invalid task type");
            panic!();
        };

        let ret_owned = std::mem::ManuallyDrop::new(ret.destroy_into_vec());

        unsafe {
            let ret_slice = std::slice::from_raw_parts_mut(
                task_data.return_values.data as *mut *const DataValue,
                task_data.return_values.len as usize,
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

fn rust_worker_execute_internal(
    actor_ptr: *mut *mut std::os::raw::c_void,
    task_type_int: i32,
    ray_function_info: RaySlice,
    args: *const *const DataValue,
    args_len: u64,
    return_values: RaySlice,
) {
    let (args_buffer, fn_name, task_type) = prepare_inputs(task_type_int, ray_function_info, args, args_len);
    let fn_str = fn_name.to_str().unwrap();

    if task_type == ray::TaskType::ACTOR_CREATION_TASK {
        let a_ptr = resolve_fn(&fn_name, fn_str, &*GLOBAL_ACTOR_CREATION_MAP).unwrap()(args_buffer);
        ray_info!("Actor Created @ address {:p} by {}", a_ptr, fn_str);
        unsafe {
            *actor_ptr = a_ptr;
        }
    } else {
        let ret = if task_type == ray::TaskType::NORMAL_TASK {
            let ret = resolve_fn(&fn_name, fn_str, &*GLOBAL_FUNCTION_MAP).unwrap()(args_buffer);
            ray_info!("Executed: {}", fn_str);
            ret
        } else if task_type == ray::TaskType::ACTOR_TASK {
            let a_ptr = ActorPtr { ptr: unsafe { *actor_ptr } };
            let ret = resolve_fn(&fn_name, fn_str, &*GLOBAL_ACTOR_METHODS_MAP).unwrap()(a_ptr, args_buffer);
            ray_info!("Executed {} on actor at {:?}", fn_str, a_ptr);
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
