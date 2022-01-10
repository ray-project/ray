#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::os::raw::*;

#[cfg(test)]
mod test {
    use super::*;
    use std::ffi::CString;
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
                c_worker_RegisterCallback(
                    Some(c_worker_execute)
                ),
                1,
                "Failed to register execute callback"
            );
        }
        // c_worker_Initialize();
    }

    // #[test]
    // fn test_init_and_shutdown() {
    //     unsafe {
    //         c_worker_RegisterCallback(Some(c_worker_execute));
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

    #[test]
    fn test_put_get() {
        unsafe {
            let mut code_search_path = CString::new("").unwrap();
            let mut head_args = CString::new("").unwrap();

            c_worker_RegisterCallback(Some(c_worker_execute));

            c_worker_InitConfig(
                1, 3, 1,
                code_search_path.as_ptr() as *mut c_char,
                head_args.as_ptr() as *mut c_char,
                0, std::ptr::null_mut()
            );
            c_worker_Initialize();

            // Create data
            let mut data_vec = vec![1u8, 2];
            let mut meta_vec = vec![3u8, 4];
            let mut data = vec![
                c_worker_AllocateDataValue(
                    data_vec.as_mut_ptr() as *mut c_void,
                    data_vec.len() as u64,
                    meta_vec.as_mut_ptr() as *mut c_void,
                    meta_vec.len() as u64,
                )
            ];

            let mut obj_ids = Vec::<*mut c_char>::new();
            obj_ids.push(std::ptr::null_mut() as *mut c_char);
            c_worker_Put(
                obj_ids.as_mut_ptr() as *mut *mut c_char,
                -1, data.as_mut_ptr(), data.len() as i32,
            );
            let c_str_id = CString::from_raw(obj_ids[0]);
            println!("{:x?}", c_str_id);

            let mut get_data: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];
            c_worker_Get(obj_ids.as_mut_ptr() as *mut *mut c_void, 1, -1, get_data.as_mut_ptr() as *mut *mut c_void);
            let slice = std::slice::from_raw_parts_mut::<u8>(
                (*(*get_data[0]).data).p as *mut u8,
                (*(*get_data[0]).data).size as usize,
            );
            assert_eq!(slice, &data_vec);
            c_worker_Shutdown();
        }
    }
}

#[no_mangle]
pub extern "C" fn c_worker_execute(
    task_type: RayInt,
    ray_function_info: RaySlice,
    args: RaySlice,
    return_values: RaySlice,
) {
}

macro_rules! apply_argc_argv {
    ($f:ident) => {
        // create a vector of zero terminated strings
        let args = std::env::args().map(|arg| CString::new(arg).unwrap() ).collect::<Vec<CString>>();
        // convert the strings to raw pointers
        let c_args = args.iter().map(|arg| arg.as_ptr()).collect::<Vec<*const c_char>>();
        // unsafe {
        // pass the pointer of the vector's internal buffer to a C function
        $f(c_args.len() as c_int, c_args.as_ptr())
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
