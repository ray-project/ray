#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(deref_nullptr)]

#[cfg(not(feature = "bazel"))]
include!(concat!(env!("OUT_DIR"), "/ray_rs_sys_bindgen.rs"));

#[cfg(feature = "bazel")]
include!(env!("BAZEL_BINDGEN_SOURCE"));

use c_vec::CVec;
use std::{ffi::CString, os::raw::*};

// #[cfg(not(feature = "bazel"))]
// mod proto;

const ID_ARRAY_LEN: usize = 28;
const NUM_WORKERS: i32 = 1;

pub type ActorID = ObjectID;
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
        // assert_eq!(ObjectID::size(), ActorID::size());
        Self(unsafe {
            CVec::new_with_dtor(ptr, ID_ARRAY_LEN, |ptr| {
                libc::free(ptr as *mut c_void);
            })
        })
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
pub type MaybeSetAsyncResultCallback = c_worker_SetAsyncResultCallback;

pub extern "C" fn rust_worker_execute_dummy(
    _task_type: RayInt,
    _ray_function_info: RaySlice,
    _args: *const *const DataValue,
    _num_args: u64,
    _return_values: RaySlice,
) {
}

pub extern "C" fn set_async_result_dummy(_fut_obj: *mut c_void, _dv: *mut DataValue) {}

pub mod ray {
    use super::*;
    pub use proto::TaskType;
    pub fn init_inner(
        is_driver: bool,
        f: MaybeExecuteCallback,
        g: MaybeSetAsyncResultCallback,
        // d: MaybeBufferDestructor,
        argc_v: Option<(c_int, *const *const c_char)>,
    ) {
        unsafe {
            let mut code_search_path = CString::new("").unwrap();
            let mut head_args = CString::new("").unwrap();

            c_worker_RegisterExecutionCallback(f);
            c_worker_RegisterSetAsyncResultCallback(g);
            // c_worker_RegisterBufferDestructor(d);

            let (argc, argv) = argc_v.unwrap_or((0, std::ptr::null()));

            c_worker_InitConfig(
                if is_driver {
                    proto::WorkerType::DRIVER
                } else {
                    proto::WorkerType::WORKER
                } as i32,
                proto::Language::RUST as i32,
                NUM_WORKERS,
                code_search_path.as_ptr() as *mut c_char,
                head_args.as_ptr() as *mut c_char,
                argc,
                argv as *mut *mut c_char,
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
        unsafe { super::c_worker_AddLocalRef(id.as_ptr()) }
    }

    pub fn remove_local_ref(id: &ObjectID) {
        unsafe { super::c_worker_RemoveLocalRef(id.as_ptr()) }
    }

    // TODO: convert this to std::fmt
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
    use protobuf::ProtobufEnum;
    use std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
    };

    pub fn parse_task_type(i: i32) -> proto::TaskType {
        ray::TaskType::from_i32(i).expect("Rust worker could not parse task type")
    }

    pub extern "C" fn set_async_result(
        ptr_to_shared: *mut c_void,
        // Instantiate with the
        d_value: *mut DataValue,
    ) -> i32 {
        // Decrease ref_count of shared by dropping it
        let shared = unsafe { Arc::from_raw(ptr_to_shared as *const Mutex<Shared>) };
        let guard = shared.lock();
        if let Ok(mut s) = guard {
            s.d_value = d_value;
            s.completed = true;
            // If we were too slow to make the first poll, wake the waker
            // as the task has been parked.
            if let Some(waker) = s.waker.take() {
                waker.wake();
            }
            return 0;
        } else {
            return 1;
        }
    }

    pub struct DataValueFuture {
        shared: Arc<Mutex<Shared>>,
    }

    impl DataValueFuture {
        fn new(id: &ObjectID) -> Self {
            let shared = Arc::new(Mutex::new(Shared {
                d_value: std::ptr::null_mut(),
                completed: false,
                waker: None,
            }));
            unsafe {
                c_worker_GetAsync(id.as_ptr(), Arc::into_raw(shared.clone()) as *mut c_void);
            }

            DataValueFuture { shared }
        }
    }

    // Since we are doing a callback within the same binary
    // (rust_worker), this is FFI-safe...
    struct Shared {
        d_value: *mut DataValue,
        waker: Option<Waker>,
        completed: bool,
    }

    impl Future for DataValueFuture {
        type Output = DataValue;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut s = self.shared.lock().unwrap();
            if s.completed {
                Poll::Ready(unsafe { *s.d_value })
            } else {
                s.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    pub fn get_async(id: &ObjectID) -> DataValueFuture {
        DataValueFuture::new(id)
    }

    // One can use Vec<&'a[u8]> in the function signature instead since SubmitTask is synchronous?
    pub fn submit_task(
        maybe_actor_id: Option<&ActorID>,
        fn_name: CString,
        args: &[&[u8]],
    ) -> ObjectID {
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

            let (actor_id_ptr, task_type) = if let Some(id) = maybe_actor_id {
                (id.as_ptr(), proto::TaskType::ACTOR_TASK)
            } else {
                (std::ptr::null(), proto::TaskType::NORMAL_TASK)
            };

            c_worker_SubmitTask(
                task_type as i32,
                actor_id_ptr,
                fn_name.as_ptr(),
                is_refs.as_mut_ptr(),
                data.as_ptr(),
                std::ptr::null_mut::<*const c_char>(),
                data.len() as i32,
                1,
                obj_ids.as_mut_ptr(),
            );

            for &dv in data.iter() {
                c_worker_DeallocateDataValue(dv);
            }

            let id = ObjectID::new(obj_ids[0]);
            println!("ObjectID: {}", util::pretty_print_id(&id));
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
                d_value.as_ptr() as *mut *mut DataValue,
            );
            *d_value[0] as DataValue
        }
    }

    pub fn create_actor(fn_name: CString, args: &[&[u8]], is_async: bool) -> ActorID {
        // Create data
        let mut meta_vec = vec![0u8];
        let mut data = args
            .iter()
            .map(|data_vec| {
                unsafe {
                    c_worker_AllocateDataValue(
                        // Why is this a void pointer, not a void/char ptr?
                        (*data_vec).as_ptr(),
                        data_vec.len() as u64,
                        std::ptr::null(),
                        0u64,
                    )
                }
            })
            .collect::<Vec<*const DataValue>>();

        let mut actor_ids = vec![std::ptr::null_mut()];
        let mut is_refs = vec![false; args.len()];

        unsafe {
            c_worker_CreateActor(
                fn_name.as_ptr(),
                is_refs.as_ptr(),
                data.as_ptr(),
                std::ptr::null_mut::<*const c_char>(),
                data.len() as i32,
                actor_ids.as_mut_ptr(),
                is_async,
            );
        }

        for &dv in data.iter() {
            unsafe {
                c_worker_DeallocateDataValue(dv);
            }
        }

        let id = ActorID::new(actor_ids[0]);
        println!("ActorID: {}", util::pretty_print_id(&id));
        id
    }
}

pub fn dv_as_slice<'a>(data: DataValue) -> &'a [u8] {
    unsafe { std::slice::from_raw_parts::<u8>((*data.data).p, (*data.data).size as usize) }
}

// Fiber Event is basically a (thread-safe) channel that
//
#[derive(Clone, Copy)]
pub struct FiberEvent {
    fiber_event_ptr: *mut c_void,
}

// This struct's methods does not modify self, they merely modify
// the FiberEvent its ptr is pointing to, which has its own Send/Sync
// guards
impl FiberEvent {
    pub fn new() -> Self {
        Self {
            fiber_event_ptr: unsafe { c_worker_CreateFiberEvent() },
        }
    }

    pub fn notify_ready(&self) {
        unsafe {
            c_worker_NotifyReady(self.fiber_event_ptr);
        }
    }

    pub fn yield_and_await(&self) {
        unsafe {
            c_worker_YieldFiberAndAwait(self.fiber_event_ptr);
        }
    }
}

// TODO: why is this correct?
unsafe impl std::marker::Send for FiberEvent {}
unsafe impl std::marker::Sync for FiberEvent {}

#[cfg(test)]
pub mod test {
    use super::*;
    #[test]
    fn test_allocate_data() {
        let mut data_vec = vec![1u8, 2];
        let mut meta_vec = vec![3u8, 4];
        unsafe {
            let data = c_worker_AllocateDataValue(
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
                c_worker_RegisterExecutionCallback(Some(rust_worker_execute_dummy)),
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
        ray::init_inner(
            true,
            Some(rust_worker_execute_dummy),
            Some(set_async_result_dummy),
            None,
        );
        unsafe {
            // Create data
            let mut data_vec = vec![1u8, 2];
            let mut meta_vec = vec![3u8, 4];
            let mut data = vec![c_worker_AllocateDataValue(
                data_vec.as_ptr(),
                data_vec.len() as u64,
                meta_vec.as_ptr(),
                meta_vec.len() as u64,
            )];

            let mut obj_ids = Vec::<*mut c_char>::new();
            obj_ids.push(std::ptr::null_mut() as *mut c_char);

            c_worker_Put(
                obj_ids.as_mut_ptr(),
                -1,
                data.as_mut_ptr(),
                data.len() as i32,
            );

            let id = ObjectID::new(obj_ids[0]);
            println!("{:?}", id);

            let mut get_data: Vec<*mut DataValue> = vec![std::ptr::null_mut() as *mut _];

            c_worker_Get(
                obj_ids.as_ptr() as *const *const c_char,
                1,
                -1,
                get_data.as_mut_ptr() as *mut *mut DataValue,
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
    unsafe { std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(ptr, len as usize)) }
}
