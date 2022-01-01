
use crate::*;
use paste::paste;

// ray::register!(add_two_vecs);
// ray::register!(add_three_vecs);
//
// ray::task!(add_two_vecs);



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
                pub fn remote<$([<$argp Borrow>]: std::borrow::Borrow<$argp>),*>(
                    &self,
                    $($arg: [<$argp Borrow>]),*
                ) -> UniquePtr<ray_api_ffi::ObjectID>
                {
                    let mut task_args = Vec::new();
                    $(
                        let result = rmp_serde::to_vec($arg.borrow()).unwrap();
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
                        String::from(
                            // concat!(module_path!(), "::", // we are temporarily disabling this
                            // When using proc macros we can modify the extern "C" fn name itself to include the
                            // module path
                            // Else, one could also reasonably mangle for Ray namespace using a random string...
                            stringify!([<ray_rust_ffi_ $name>]))
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

pub fn get<R: serde::de::DeserializeOwned>(id : UniquePtr<ray_api_ffi::ObjectID>) -> R {
    let buf = ray_api_ffi::GetRaw(id);
    rmp_serde::from_read_ref::<_, R>(&buf).unwrap()
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
pub fn add_two_vecs_nested(a: Vec<u64>, b: Vec<u64>) -> Vec<u64> {
    get(add_two_vecs.remote(a, b))
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
