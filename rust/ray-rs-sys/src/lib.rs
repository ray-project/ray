#![feature(fn_traits, unboxed_closures, min_specialization)]

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

use paste::paste;
use uniffi::ffi::RustBuffer;
use core::pin::Pin;
use cxx::{let_cxx_string, CxxString, UniquePtr, SharedPtr, CxxVector};

pub struct RustTaskArg {
    buf: Vec<u8>,
    object_id: String,
}

impl RustTaskArg {
    pub fn is_value(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn value(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn object_ref(&self) -> &str {
        &self.object_id
    }
}


use std::{collections::HashMap, sync::Mutex};

type InvokerFunction = extern "C" fn(RustBuffer) -> RustBuffer;

type FunctionPtrMap = HashMap<Vec<u8>, InvokerFunction>;



// TODO (jon-chuang): ensure you know how to handle the lifetime of RustBuffer safely...
// Understand how to safely handle lifetimes for FFI more generally.

// Here, the RustBuffer ought to be passed by value (move semantics) and thus disappear
// from scope on the Rust side.

// We need to handle the case where the key is not found with an error;
// however, we could also check in advance via the initially loaded function name mapping

// If we were to apply the function for the user directly in the application code
// and return the buffer (as was suggested for the C++ API), we would probably also
// reduce the attack surface
#[no_mangle]
extern "C" fn get_function_ptr(key: RustBuffer) -> Option<InvokerFunction> {
    let key_as_vec = key.destroy_into_vec();
    GLOBAL_FUNCTION_MAP.lock().unwrap().get(&key_as_vec).cloned()
}

// struct FunctionManager {
//     cache: FunctionPtrMap,
//     libs:
// }

#[repr(C)]
struct BufferData {
    ptr_u64: u64,
    size: u64,
}

type BufferRawPartsVec = Vec<(u64, u64)>;
struct ArgsBuffer(BufferRawPartsVec);

// TODO: maybe this more legit way...
impl ArgsBuffer {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add_arg(&mut self, ptr: *const u8, size: usize) {
        self.0.push((ptr as u64, size as u64));
    }

    pub fn to_rust_buffer(self) -> RustBuffer {
        RustBuffer::from_vec(rmp_serde::to_vec(&self.0).unwrap())
    }
}

// unsafe fn push_arg_slice_from_raw_parts(vec: &mut ArgsVec<'a>, data: *const u8, size: usize) {
//     vec.push((data, size as u64));
// }

fn get_execute_result(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8> {
    let args = RustBuffer::from_vec(rmp_serde::to_vec(&(&args, &sizes)).unwrap());
    let ret = GLOBAL_FUNCTION_MAP.lock().unwrap().get(&fn_name.as_bytes().to_vec()).unwrap()(args);
    ret.destroy_into_vec()
}

fn new_vec() -> Vec<u64> {
    Vec::<u64>::new()
}


#[cxx::bridge(namespace = "ray")]
pub mod ray_api_ffi {
    extern "Rust" {
        type RustTaskArg;
        fn is_value(&self) -> bool;
        fn value(&self) -> &Vec<u8>;
        fn object_ref(&self) -> &str;
    }

    extern "Rust" {
        fn new_vec() -> Vec<u64>;
        fn get_execute_result(args: Vec<u64>, sizes: Vec<u64>, fn_name: &CxxString) -> Vec<u8>;
    }

    // extern "Rust" {
    //     unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8>;
    // }

    unsafe extern "C++" {
        include!("rust/ray-rs-sys/cpp/tasks.h");

        type ObjectID;

        // fn Binary(self: &ObjectID) -> &CxxString;

        fn Submit(name: &str, args: &Vec<RustTaskArg>) -> UniquePtr<ObjectID>;
        // fn InitRust();
    }

    unsafe extern "C++" {
        include!("ray/api.h");
        include!("rust/ray-rs-sys/cpp/wrapper.h");

        type Uint64ObjectRef;
        type StringObjectRef;

        fn InitAsLocal();
        fn Init();
        fn IsInitialized() -> bool;
        fn Shutdown();

        fn PutUint64(obj: u64) -> UniquePtr<Uint64ObjectRef>;
        fn GetUint64(obj_ref: UniquePtr<Uint64ObjectRef>) -> SharedPtr<u64>;

        fn PutString(obj: &CxxString) -> UniquePtr<StringObjectRef>;
        fn GetString(obj_ref: UniquePtr<StringObjectRef>) -> SharedPtr<CxxString>;

        fn PutAndGetConfig();

        fn ID(self: &Uint64ObjectRef) -> &CxxString;
        fn ID(self: &StringObjectRef) -> &CxxString;
    }
}


// Idea: add a macro that
macro_rules! impl_ray_function {
    ([$n:literal], $($arg:ident: $argp:ident [$argty:ty]),*) => {
        paste! {
            pub struct [<RayFunction $n>]<$($argp,)* R> {
                raw_fn: fn($($argty),*) -> R,
                wrapped_fn: InvokerFunction,
                ffi_lookup_name: String,
            }

            impl<$($argp: serde::Serialize,)* R> [<RayFunction $n>]<$($argp,)* R> {
                fn new(
                    raw_fn: fn($($argty),*) -> R,
                    wrapped_fn: InvokerFunction,
                    ffi_lookup_name: String
                ) -> Self {
                    Self { raw_fn, wrapped_fn, ffi_lookup_name }
                }

                pub fn name(&self) -> &str {
                    &self.ffi_lookup_name
                }

                pub fn call(&self, $($arg: $argty),*) -> R {
                    (self.raw_fn)($($arg),*)
                }

                // Return object ref
                // This should submit the
                pub fn remote(&self, $($arg: $argty),*) -> UniquePtr<ray_api_ffi::ObjectID> {
                    let mut task_args = Vec::new();
                    $(
                        let result = rmp_serde::to_vec(&$arg).unwrap();
                        task_args.push(RustTaskArg {
                            buf: result,
                            object_id: String::new()
                        });
                    )*
                    ray_api_ffi::Submit(&self.ffi_lookup_name, &task_args)
                }

                pub fn ray_call(&self, args: RustBuffer) -> RustBuffer {
                    (self.wrapped_fn)(args)
                }

                pub fn get_invoker(&self) -> InvokerFunction {
                    self.wrapped_fn
                }
            }

            impl<$($argp,)* R> std::ops::FnOnce<($($argty),*)> for [<RayFunction $n>]<$($argp,)* R> {
                type Output = R;
                extern "rust-call" fn call_once(self, ($($arg),*): ($($argty),*)) -> Self::Output {
                    (self.raw_fn)($($arg),*)
                }
            }

            impl<$($argp,)* R> std::ops::FnMut<($($argty),*)> for [<RayFunction $n>]<$($argp,)* R> {
                extern "rust-call" fn call_mut(&mut self, ($($arg),*): ($($argty),*)) -> Self::Output {
                    (self.raw_fn)($($arg),*)
                }
            }

            impl<$($argp,)* R> std::ops::Fn<($($argty),*)> for [<RayFunction $n>]<$($argp,)* R> {
                extern "rust-call" fn call(&self, ($($arg),*): ($($argty),*)) -> Self::Output {
                    (self.raw_fn)($($arg),*)
                }
            }
        }
    };
}

impl_ray_function!([0], );
impl_ray_function!([1], a0: T0 [T0]);
impl_ray_function!([2], a0: T0 [T0], a1: T1 [T1]);
impl_ray_function!([3], a0: T0 [T0], a1: T1 [T1], a2: T2 [T2]);
// impl_ray_function!(a0: T0, a1: T1, a2: T2, a3: T3);

macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

macro_rules! deserialize {
    ($ptrs:ident, $sizes:ident) => {};
    ($ptrs:ident, $sizes:ident, $arg:ident: $argty:ty $(,$args:ident: $argsty:ty)*) => {
        deserialize!($ptrs, $sizes $(,$args:$argsty)*);
        let ptr = $ptrs[$ptrs.len() - count!($($args)*) - 1];
        let size = $sizes[$sizes.len() - count!($($args)*) - 1];
        let $arg = rmp_serde::from_read_ref::<_, $argty>(
            unsafe { std::slice::from_raw_parts(ptr as *const u8, size as usize) }
        ).unwrap();
    };
}

macro_rules! serialize {
    ($ptrs:ident) => {};
    ($ptrs:ident, $arg:ident: $argty:ty $(,$args:ident: $argsty:ty)*) => {
        serialize!($ptrs $(,$args:$argsty)*);
        let (ptr, size) = $ptrs[$ptrs.len() - count!($($args)*) - 1];
        let $arg = rmp_serde::from_read_ref::<_, $argty>(
            unsafe { std::slice::from_raw_parts(ptr as *const u8, size as usize) }
        ).unwrap();
    };
}

lazy_static::lazy_static! {
    static ref GLOBAL_FUNCTION_MAP: Mutex<FunctionPtrMap> =
        Mutex::new([
            ("ray_rs_sys::add_two_vecs".as_bytes().to_vec(), add_two_vecs.get_invoker()),
        ].iter().cloned().collect());
}


// When porting this to
#[macro_export]
macro_rules! remote_internal {
    ($lit_n:literal, $name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block) => {
        paste! {
            fn [<ray_rust_private_ $name>]($($arg: $argty,)*) -> $ret {
                $body
            }

            // TODO: convert this `no_mangle` name to use module_path!():: $name
            #[no_mangle]
            extern "C" fn [<ray_rust_ffi_ $name>](args: RustBuffer) -> RustBuffer {
                let (arg_raw_ptrs, sizes) = rmp_serde::from_read_ref::<_, (Vec<u64>, Vec<u64>)>(
                    &args.destroy_into_vec()
                ).unwrap();
                // TODO: ensure this
                deserialize!(arg_raw_ptrs, sizes $(,$arg:$argty)*);
                let ret: $ret = [<ray_rust_private_ $name>]($($arg,)*);
                let result = rmp_serde::to_vec(&ret).unwrap();
                RustBuffer::from_vec(result)
            }

            lazy_static::lazy_static! {
                pub static ref $name: [<RayFunction $lit_n>]<$($argty,)* $ret>
                    = [<RayFunction $lit_n>]::new(
                        [<ray_rust_private_ $name>],
                        [<ray_rust_ffi_ $name>],
                        String::from(concat!(module_path!(), "::", stringify!([<ray_rust_ffi_ $name>])))
                    );
            }
        }
    };
}

macro_rules! match_remote {
    ([$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(0, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(1, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt $y:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(2, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt $y:tt $z:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(3, $name ($($arg: $argty),*) -> $ret $body); };
    // ($x:tt $y:tt $z: tt $a:tt) => remote_internal!(4, $name($($arg: $argty),*) -> $ret $body);
}

#[macro_export]
macro_rules! remote {
    (pub fn $name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block) => {
        match_remote!($($arg)* [$name ($($arg: $argty),*) -> $ret $body]);
    }
}

remote! {
    pub fn add_two_vecs(a: Vec<u64>, b: Vec<u64>) -> Vec<u64> {
        assert_eq!(a.len(), b.len());
        let mut ret = vec![0u64; a.len()];
        for i in 0..a.len() {
            ret[i] = a[i] + b[i];
        }
        ret
    }
}

remote! {
    pub fn add_three_vecs(a: Vec<u64>, b: Vec<u64>, c: Vec<u64>) -> Vec<u64> {
        assert_eq!(a.len(), b.len());
        assert_eq!(a.len(), c.len());
        let mut ret = vec![0u64; a.len()];
        for i in 0..a.len() {
            ret[i] = a[i] + b[i] + c[i];
        }
        ret
    }
}

unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8> {
    let mut vec = Vec::with_capacity(size);
    let slice = core::slice::from_raw_parts(data, size);
    vec.copy_from_slice(slice);
    vec
}

// struct ObjectRef<T> {
//     _type: PhantomData<T>
// }

// fn main() {
//     use math::vectors::add_two_vecs;
//     add_two_vecs.remote(a, b);
//
// }
