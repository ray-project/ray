#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]

#[cfg(not(feature = "bazel"))]
include!(concat!(env!("OUT_DIR"), "/ray_rs_sys_bindgen.rs"));

#[cfg(feature = "bazel")]
include!(env!("BAZEL_BINDGEN_SOURCE"));

use std::os::raw::*;
use std::ffi::CString;
use c_vec::CVec;

#[cfg(not(feature = "bazel"))]
mod proto;

const ID_ARRAY_LEN: usize = 28;
const NUM_WORKERS: i32 = 1;

pub struct ObjectID(CVec<c_char>);

impl std::fmt::Debug for ObjectID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.as_slice()
                .iter()
                .map(|x| format!("{:02x?}", x))
                .collect::<Vec<_>>()
                .join(""),
        )
    }
}

impl ObjectID {
    fn new(ptr: *mut c_char) -> Self {
        Self(
            unsafe {
                CVec::new_with_dtor(ptr, ID_ARRAY_LEN, |ptr| {
                    libc::free(ptr as *mut c_void);
                })
            }
        )
    }

    fn as_slice(&self) -> &[c_char] {
        self.0.as_ref()
    }

    fn as_ptr(&self) -> *const c_char {
        self.0.as_ref().as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut c_char {
        self.0.as_mut().as_mut_ptr()
    }
}

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
    _args: *const *const DataValue,
    _num_args: u64,
    _return_values: RaySlice,
) {
}

pub mod ray {
    use super::*;
    pub fn init_inner(
        is_driver: bool,
        f: MaybeExecuteCallback,
        // d: MaybeBufferDestructor,
        argc_v: Option<(c_int, *const *const c_char)>
    ) {
        unsafe {
            let mut code_search_path = CString::new("").unwrap();
            let mut head_args = CString::new("").unwrap();

            c_worker_RegisterExecutionCallback(f);
            // c_worker_RegisterBufferDestructor(d);

            let (argc, argv) = argc_v.unwrap_or((0, std::ptr::null()));

            c_worker_InitConfig(
                if is_driver {
                    proto::WorkerType::DRIVER
                } else {
                    proto::WorkerType::WORKER
                } as i32,
                proto::Language::RUST as i32, NUM_WORKERS,
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

pub mod util {
    use super::*;
    pub fn add_local_ref(id: &ObjectID) {
        unsafe {
            super::c_worker_AddLocalRef(id.as_ptr())
        }
    }

    pub fn remove_local_ref(id: &ObjectID) {
        unsafe {
            super::c_worker_RemoveLocalRef(id.as_ptr())
        }
    }

    pub fn pretty_print_id(id: &ObjectID) -> String {
        id.as_slice()
            .iter()
            .map(|x| format!("{:02x?}", x))
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn log_internal(msg: String) {
        unsafe {
            super::c_worker_Log(std::ffi::CString::new(msg).unwrap().into_raw());
        }
    }
//     pub fn fd_to_cstring(fd: RaySlice) -> CString {
//         CString::from(fd.data as *c_char)
//     }
}


pub mod internal {
    use super::*;
    // One can use Vec<&'a[u8]> in the function signature instead since SubmitTask is synchronous?
    pub fn submit<'a>(fn_name: CString, args: &[&[u8]]) -> ObjectID {
        unsafe {
            // Create data
            let mut meta_vec = vec![0u8];
            let mut data = args
                .iter()
                .map(|data_vec| {
                    c_worker_AllocateDataValue(
                        // Why is this a void pointer, not a void/char ptr?
                        (*data_vec).as_ptr(),
                        data_vec.len() as u64,
                        std::ptr::null(),
                        0u64,
                    )
                })
                .collect::<Vec<*const DataValue>>();

            let mut obj_ids = vec![std::ptr::null_mut()];
            let mut is_refs = vec![false; args.len()];

            c_worker_SubmitTask(
                fn_name.into_raw(),
                is_refs.as_mut_ptr(),
                data.as_ptr(),
                std::ptr::null_mut::<*mut c_char>(),
                data.len() as i32,
                1,
                obj_ids.as_mut_ptr()
            );

            for &dv in data.iter() {
                c_worker_DeallocateDataValue(dv);
            }

            let id = ObjectID::new(obj_ids[0]);
            println!("ObjectID: {:x?}", util::pretty_print_id(&id));
            id
        }
    }

    pub fn get_slice<'a>(id: &ObjectID, timeout: i32) -> &'a [u8] {
        dv_as_slice(get(id, timeout))
    }

    // TODO: change to `get_data_value`
    #[inline]
    fn get(id: &ObjectID, timeout: i32) -> DataValue {
        let mut data = vec![id.as_ptr()];
        let mut d_value: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];
        unsafe {
            c_worker_Get(
                data.as_ptr(),
                1,
                timeout,
                d_value.as_ptr() as *mut *mut DataValue
            );
            *d_value[0] as DataValue
        }
    }
}

pub fn dv_as_slice<'a>(data: DataValue) -> &'a [u8] {
    unsafe {
        std::slice::from_raw_parts::<u8>(
            (*data.data).p,
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
                    data_vec.as_ptr(),
                    data_vec.len() as u64,
                    meta_vec.as_ptr(),
                    meta_vec.len() as u64,
                );
            assert_eq!((*(*data).data).p, data_vec.as_mut_ptr());
            assert_eq!((*(*data).meta).p, meta_vec.as_mut_ptr());
            assert_eq!((*(*data).data).size, data_vec.len() as u64);
            assert_eq!((*(*data).meta).size, data_vec.len() as u64);
            c_worker_DeallocateDataValue(data);
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

    #[test]
    fn test_put_get_raw() {
        ray::init_inner(true, Some(rust_worker_execute_dummy), None);
        unsafe {
            // Create data
            let mut data_vec = vec![1u8, 2];
            let mut meta_vec = vec![3u8, 4];
            let mut data = vec![
                c_worker_AllocateDataValue(
                    data_vec.as_ptr(),
                    data_vec.len() as u64,
                    meta_vec.as_ptr(),
                    meta_vec.len() as u64,
                )
            ];

            let mut obj_ids = Vec::<*mut c_char>::new();
            obj_ids.push(std::ptr::null_mut() as *mut c_char);

            c_worker_Put(
                obj_ids.as_mut_ptr() as *mut *mut c_char,
                -1, data.as_mut_ptr() as *mut *mut DataValue, data.len() as i32,
            );

            let id = ObjectID::new(obj_ids[0]);
            println!("{:?}", id);

            let mut get_data: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];

            c_worker_Get(
                obj_ids.as_ptr() as *const *const c_char,
                1, -1,
                get_data.as_mut_ptr() as *mut *mut DataValue
            );

            let slice = std::slice::from_raw_parts_mut::<u8>(
                (*(*get_data[0]).data).p as *mut u8,
                (*(*get_data[0]).data).size as usize,
            );
            assert_eq!(slice, &data_vec);

            assert_eq!(internal::get_slice(&id, -1), &data_vec);

            for &d in data.iter() {
                c_worker_DeallocateDataValue(d);
            }

            c_worker_Shutdown();
        }
    }
}

type BufferDestructor = extern "C" fn(*mut u8, u64);

// This is how to prevent memory leakage...
// How does Rust allocate memory...? In terms of malloc slices?
// Apprently, in terms of malloc slices but in the layout of a type....
//
// TODO: Doesn't need to be extern?
pub extern "C" fn rust_raw_parts_dealloc(ptr: *mut u8, len: u64) {
    unsafe {
        std::ptr::drop_in_place(
            std::ptr::slice_from_raw_parts_mut(ptr, len as usize)
        )
    }
}
