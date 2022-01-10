#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));


#[test]
fn test_allocate_data() {
    use std::os::raw::*;
    let mut data_vec = vec![1, 2];
    let mut meta_vec = vec![3, 4];
    unsafe {
        let data =
            rust_worker_AllocateDataValue(
                data_vec.as_mut_ptr() as *mut c_void,
                data_vec.len() as u64,
                meta_vec.as_mut_ptr() as *mut c_void,
                meta_vec.len() as u64,
            );
        assert_eq!((*(*data).data).p, data_vec.as_mut_ptr() as *mut c_void);
        assert_eq!((*(*data).meta).p, meta_vec.as_mut_ptr() as *mut c_void);
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
// pub extern "C" fn rust_worker_execute(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8> {
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
