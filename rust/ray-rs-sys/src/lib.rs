#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]

#[cfg(not(feature = "bazel"))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(feature = "bazel")]
include!(env!("BAZEL_BINDGEN_SOURCE"));

use std::os::raw::*;
use std::ffi::CString;

// pub type execute_function

struct LaunchConfig {
    is_driver: bool,
    code_search_path: CString,
    head_args: CString,
}

pub type MaybeExecuteCallback = c_worker_ExecuteCallback;

pub extern "C" fn rust_worker_execute_dummy(
    _task_type: RayInt,
    _ray_function_info: RaySlice,
    _args: RaySlice,
    _return_values: RaySlice,
) {
}

pub mod ray {
    use super::*;
    pub fn init_inner(
        is_driver: bool,
        f: MaybeExecuteCallback,
        argc_v: Option<(c_int, *const *const c_char)>
    ) {
        unsafe {
            let mut code_search_path = CString::new("").unwrap();
            let mut head_args = CString::new("").unwrap();

            c_worker_RegisterExecutionCallback(f);

            let (argc, argv) = argc_v.unwrap_or((0, std::ptr::null()));

            c_worker_InitConfig(
                if is_driver { 1 } else { 0 }, 3, 1,
                code_search_path.as_ptr() as *mut c_char,
                head_args.as_ptr() as *mut c_char,
                argc, argv as *mut *mut c_char,
            );
            c_worker_Initialize();
        }
    }

    pub fn shutdown() {
        unsafe {
            c_worker_Shutdown();
        }
    }

    pub fn run() {
        unsafe {
            c_worker_Run();
        }
    }
}

pub fn get(id: CString, timeout: i32) -> DataValue {
    let mut data = vec![id.as_ptr()];
    let mut d_value: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];
    unsafe {
        c_worker_Get(
            data.as_ptr() as *mut *mut c_char,
            1,
            timeout,
            d_value.as_ptr() as *mut *mut DataValue
        );
        *d_value[0] as DataValue
    }
}

pub fn dv_as_slice<'a>(data: DataValue) -> &'a mut [u8] {
    unsafe {
        std::slice::from_raw_parts_mut::<u8>(
            (*data.data).p as *mut u8,
            (*data.data).size as usize,
        )
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    #[test]
    fn test_allocate_data() {
        let mut data_vec = vec![1u8, 2];
        let mut meta_vec = vec![3u8, 4];
        unsafe {
            let data =
                c_worker_AllocateDataValue(
                    data_vec.as_mut_ptr() as *mut c_void,
                    data_vec.len() as u64,
                    meta_vec.as_mut_ptr() as *mut c_void,
                    meta_vec.len() as u64,
                );
            assert_eq!((*(*data).data).p, data_vec.as_mut_ptr() as *mut c_void);
            assert_eq!((*(*data).meta).p, meta_vec.as_mut_ptr() as *mut c_void);
            assert_eq!((*(*data).data).size, data_vec.len() as u64);
            assert_eq!((*(*data).meta).size, data_vec.len() as u64);
        }
    }

    #[test]
    fn test_register_callback() {
        unsafe {
            assert_eq!(
                c_worker_RegisterExecutionCallback(
                    Some(rust_worker_execute_dummy)
                ),
                1,
                "Failed to register execute callback"
            );
        }
    }

    // #[test]
    // fn test_init_and_shutdown() {
    //     unsafe {
    //         c_worker_RegisterExecutionCallback(Some(c_worker_execute));
    //         let mut code_search_path = CString::new("").unwrap();
    //         let mut head_args = CString::new("").unwrap();
    //         c_worker_InitConfig(
    //             1, 3, 1,
    //             code_search_path.as_ptr() as *mut c_char,
    //             head_args.as_ptr() as *mut c_char,
    //             0, std::ptr::null_mut()
    //         );
    //         c_worker_Initialize();
    //         c_worker_Shutdown();
    //     }
    // }

    // #[test]
    // fn test_put_get_raw() {
    //     ray::init_inner(true, Some(rust_worker_execute_dummy), None);
    //     unsafe {
    //         // Create data
    //         let mut data_vec = vec![1u8, 2];
    //         let mut meta_vec = vec![3u8, 4];
    //         let mut data = vec![
    //             c_worker_AllocateDataValue(
    //                 data_vec.as_mut_ptr() as *mut c_void,
    //                 data_vec.len() as u64,
    //                 meta_vec.as_mut_ptr() as *mut c_void,
    //                 meta_vec.len() as u64,
    //             )
    //         ];
    //
    //         let mut obj_ids = Vec::<*mut c_char>::new();
    //         obj_ids.push(std::ptr::null_mut() as *mut c_char);
    //
    //         c_worker_Put(
    //             obj_ids.as_mut_ptr() as *mut *mut c_char,
    //             -1, data.as_mut_ptr(), data.len() as i32,
    //         );
    //
    //         let c_str_id = CString::from_raw(obj_ids[0]);
    //         println!("{:x?}", c_str_id);
    //
    //         let mut get_data: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];
    //
    //         c_worker_Get(
    //             obj_ids.as_mut_ptr() as *mut *mut c_char,
    //             1, -1,
    //             get_data.as_mut_ptr() as *mut *mut DataValue
    //         );
    //
    //         let slice = std::slice::from_raw_parts_mut::<u8>(
    //             (*(*get_data[0]).data).p as *mut u8,
    //             (*(*get_data[0]).data).size as usize,
    //         );
    //         assert_eq!(slice, &data_vec);
    //
    //         assert_eq!(dv_as_slice(get(c_str_id, -1)), &data_vec);
    //
    //         c_worker_Shutdown();
    //     }
    // }

    #[test]
    fn test_submit_task() {
        ray::init_inner(true, Some(rust_worker_execute_dummy), None);
        unsafe {
            // Create data
            let mut data_vec = vec![1u8, 2];
            let mut meta_vec = vec![0u8];
            let mut data = vec![
                c_worker_AllocateDataValue(
                    data_vec.as_mut_ptr() as *mut c_void,
                    data_vec.len() as u64,
                    meta_vec.as_mut_ptr() as *mut c_void,
                    meta_vec.len() as u64,
                )
            ];

            assert_eq!((*(*data[0]).data).size, 2);

            let mut obj_ids = vec![std::ptr::null_mut() as *mut c_char];
            c_worker_SubmitTask(
                CString::new("").unwrap().as_ptr() as *mut c_char,
                &mut false as *mut bool,
                data.as_mut_ptr(),
                std::ptr::null_mut::<*mut c_char>(),
                data.len() as i32,
                1,
                obj_ids.as_mut_ptr()
            );

            let c_str_id = CString::from_raw(obj_ids[0]);
            println!("{:x?}", c_str_id);

            let mut get_data: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];

            c_worker_Get(
                obj_ids.as_mut_ptr() as *mut *mut c_char,
                1, -1,
                get_data.as_mut_ptr() as *mut *mut DataValue
            );

            // let slice = std::slice::from_raw_parts_mut::<u8>(
            //     (*(*get_data[0]).data).p as *mut u8,
            //     (*(*get_data[0]).data).size as usize,
            // );
            // assert_eq!(slice, &vec![3u8]);
            //
            // assert_eq!(dv_as_slice(get(c_str_id, -1)), &vec![3u8]);

            c_worker_Shutdown();
        }
    }
}

// TODO (jon-chuang): improve the function signature here
pub extern "C" fn rust_worker_execute_add(
    _task_type: RayInt,
    _ray_function_info: RaySlice,
    args: RaySlice,
    mut return_values: RaySlice,
) {
    unsafe {
        let data = std::slice::from_raw_parts(
            args.data as *mut *mut DataValue,
            args.len as usize,
        );
        let slice = dv_as_slice(*data[0]);

        let ret_slice = std::slice::from_raw_parts(
            return_values.data as *mut *mut DataValue,
            return_values.len as usize,
        );


        let mut ret_owned = vec![slice[0] + slice[1]];

        let mut data_buffer_owned = DataBuffer {
            size: ret_owned.len() as u64,
            p: ret_owned.as_mut_ptr() as *mut c_void,
        };
        (*ret_slice[0]).data = &mut data_buffer_owned;

        // Reimplement RustBuffer functionality around the raw type
        std::mem::forget(ret_owned);
        std::mem::forget(data_buffer_owned);
    }
}

// func (or *ObjectRef) Get() ([]interface{}, error) {
//     util.Logger.Debugf("get result returObjectIdArrayPointer:%v ,return object num:%d", or.returnObjectIds, or.returnObjectNum)
//     if or.returnObjectNum == 0 {
//         return nil, nil
//     }
//     returnValues := make([]unsafe.Pointer, or.returnObjectNum, or.returnObjectNum)
//     success := C.go_worker_Get((*unsafe.Pointer)(or.returnObjectIds), C.int(or.returnObjectNum), C.int(-1), &returnValues[0])
//     if success != 0 {
//         panic("failed to get task result")
//     }
//     returnGoValues := make([]interface{}, or.returnObjectNum, or.returnObjectNum)
//     for index, returnValue := range returnValues {
//         rv := (*C.struct_DataValue)(returnValue)
//         goValue, err := dataValueToGoValue(rv)
//         if err != nil {
//             return nil, err
//         }
//         returnGoValues[index] = goValue
//     }
//     return returnGoValues, nil
// }


// #[no_mangle]
// pub extern "C" fn c_worker_execute(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8> {
//     let args_buffer = RustBuffer::from_vec(rmp_serde::to_vec(&(&args, &sizes)).unwrap());
//     // Check if we get a cache hit
//     let libs = LIBRARIES.lock().unwrap();
//
//     let mut fn_map = GLOBAL_FUNCTION_MAP.lock().unwrap();
//
//     let mut ret_ref = fn_map.get(&fn_name.as_bytes().to_vec());
//     // Check if we can get fn from available libraries
//
//     // TODO(jon-chuang): figure out if you can narrow search
//     // by mapping library name to function crate name...
//     if let None = ret_ref {
//         for lib in libs.iter() {
//             let ret = unsafe {
//                     lib.get::<InvokerFunction>(fn_name.to_str().unwrap().as_bytes()).ok()
//             };
//             ray_api_ffi::LogInfo(&format!("Loaded function {:?} as {:?}", fn_name.to_str().unwrap(), ret));
//             if let Some(symbol) = ret {
//                 let static_symbol = unsafe {
//                     std::mem::transmute::<Symbol<_, >, Symbol<'static, InvokerFunction>>(symbol)
//                 };
//                 fn_map.insert(fn_name.as_bytes().to_vec(), static_symbol);
//                 ret_ref = fn_map.get(&fn_name.as_bytes().to_vec());
//             }
//         }
//     } else {
//         ray_api_ffi::LogInfo(&format!("Using cached library symbol for {:?}: {:?}", fn_name.to_str().unwrap(), ret_ref));
//     }
//     let ret = ret_ref.expect(&format!("Could not find symbol for fn of name {:?}", fn_name))(args_buffer);
//     ret.destroy_into_vec()
// }
