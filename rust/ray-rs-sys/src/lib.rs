#[cxx::bridge(namespace = "ray::core")]
mod ray_ffi {
    unsafe extern "C++" {
        include!("ray/core_worker/core_worker.h");
        include!("ray/core_worker/core_worker_options.h");
        include!("ray/common/id.h");

        type CoreWorker;
        type CoreWorkerOptions;
        #[namespace = "ray"]
        type WorkerID;

        fn ConnectToRaylet(self: Pin<&mut CoreWorker>);
        fn Shutdown(self: Pin<&mut CoreWorker>);
        fn GetWorkerID(self: &CoreWorker) -> &WorkerID;
    }
}

use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

#[cxx::bridge(namespace = "ray")]
pub mod ray_api_ffi {
    unsafe extern "C++" {
        include!("ray/api.h");
        include!("rust/ray-rs-sys/cpp/wrapper.h");
        include!("rust/ray-rs-sys/cpp/tasks.h");

        type Uint64ObjectRef;
        type StringObjectRef;
        type TaskArgs;
        type ObjectID;

        fn InitAsLocal();
        fn Init();
        fn IsInitialized() -> bool;
        fn Shutdown();

        fn Submit(name: &CxxString, args: UniquePtr<CxxVector<TaskArgs>>) -> UniquePtr<ObjectID>;

        fn PutUint64(obj: u64) -> UniquePtr<Uint64ObjectRef>;
        fn GetUint64(obj_ref: UniquePtr<Uint64ObjectRef>) -> SharedPtr<u64>;

        fn PutString(obj: &CxxString) -> UniquePtr<StringObjectRef>;
        fn GetString(obj_ref: UniquePtr<StringObjectRef>) -> SharedPtr<CxxString>;

        fn PutAndGetConfig();

        fn ID(self: &Uint64ObjectRef) -> &CxxString;
        fn ID(self: &StringObjectRef) -> &CxxString;
    }
}

// struct ObjectRef<T> {
//     _type: PhantomData<T>
// }
