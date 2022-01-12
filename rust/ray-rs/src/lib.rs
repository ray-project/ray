#![feature(fn_traits, unboxed_closures, min_specialization)]
#[macro_use]
pub use paste::paste;
#[macro_use]
pub use lazy_static::lazy_static;
pub use rmp_serde;
pub use uniffi::ffi::RustBuffer;
use core::pin::Pin;


pub use ray_rs_sys::*;
pub use ray_rs_sys::ray;

pub mod remote_functions;
pub use remote_functions::*;

pub use std::ffi::CString;

use std::{collections::HashMap, sync::Mutex, os::raw::c_char, ops::Deref};

use libloading::{Library, Symbol};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;

type FunctionPtrMap = HashMap<CString, Symbol<'static, InvokerFunction>>;


#[macro_export]
macro_rules! ray_log {
    ($msg:expr) => {
        util::log_internal(format!("[rust] {}:{}: {}", file!(), line!(), $msg));
    }
}

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
            Some(lib) => libs.push(lib),
            None => panic!("Shared-object library not found at path: {}", path),
        }
    }
}

pub extern "C" fn rust_worker_execute(
    _task_type: RayInt,
    ray_function_info: RaySlice,
    args: RaySlice,
    return_values: RaySlice,
) {
    // TODO (jon-chuang): One should replace RustBuffer with RaySlice...
    // TODO (jon-chuang): Try to move unsafe into ray_rs_sys
    let args_slice = unsafe {
        std::slice::from_raw_parts(
            args.data as *mut *mut DataValue,
            args.len as usize,
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
    // Check if we get a cache hit
    let libs = LIBRARIES.lock().unwrap();
    let mut fn_map = GLOBAL_FUNCTION_MAP.lock().unwrap();

    let mut ret_ref = fn_map.get(fn_name.deref());
    // Check if we can get fn from available libraries

    // TODO(jon-chuang): figure out if you can narrow search
    // by mapping library name to function crate name...
    if let None = ret_ref {
        for lib in libs.iter() {
            let ret = unsafe {
                    lib.get::<InvokerFunction>(fn_name.to_str().unwrap().as_bytes()).ok()
            };
            ray_log!(format!("Loaded function {:?} as {:?}", fn_name.to_str().unwrap(), ret));
            if let Some(symbol) = ret {
                let static_symbol = unsafe {
                    std::mem::transmute::<Symbol<_, >, Symbol<'static, InvokerFunction>>(symbol)
                };
                fn_map.insert(fn_name.deref().clone(), static_symbol);
                ret_ref = fn_map.get(fn_name.deref());
            }
        }
    } else {
        ray_log!(format!("Using cached library symbol for {:?}: {:?}", fn_name.to_str().unwrap(), ret_ref));
    }
    let func = ret_ref.expect(&format!("Could not find symbol for fn of name {:?}", fn_name.to_str().unwrap()));
    ray_log!("executing");
    let ret = func(args_buffer);

    let mut ret_owned = std::mem::ManuallyDrop::new(ret.destroy_into_vec());

    unsafe {
        let mut dv_ptr = c_worker_AllocateDataValue(
            ret_owned.as_mut_ptr(),
            ret_owned.len() as u64,
            std::ptr::null_mut::<u8>(),
            0,
        );
        let ret_slice = std::slice::from_raw_parts(
            return_values.data as *mut *mut DataValue,
            return_values.len as usize,
        );
        let dv_ptr = c_worker_AllocateDataValue(
            // (*(*ret_slice[0]).data).p,
            ret_owned.as_mut_ptr(),
            ret_owned.len() as u64,
            std::ptr::null_mut::<u8>(),
            0,
        );
        (*ret_slice[0]).data = (*dv_ptr).data;
    }
}
