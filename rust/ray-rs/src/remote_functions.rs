use crate::*;
use paste::paste;

// Not necessary, now auto-registered...
// ray::register!(add_two_vecs);
// ray::register!(add_three_vecs);
//
// ray::task!(add_two_vecs);

// Idea: add a macro that
macro_rules! impl_ray_function {
    ([$n:literal], $($arg:ident: $argp:ident [$argty:ty]),*) => {
        paste! {
            pub struct [<RayActorMethod $n>]<$($argp,)* R> {
                phantom: std::marker::PhantomData<($($argty,)* R)>,
                ffi_lookup_name: CString,
            }

            // Have this be $argpBorrow: Borrow<argp> instead
            // So that one can accept references as function signature arguments.
            impl<$($argp: serde::Serialize,)* R> [<RayActorMethod $n>]<$($argp,)* R> {
                pub fn new(
                    // raw_fn: fn($($argty),*) -> R,
                    // wrapped_fn: InvokerFunction,
                    ffi_lookup_name: CString,
                ) -> Self {
                    Self { ffi_lookup_name, phantom: PhantomData }
                }

                pub fn name<'a>(&'a self) -> &'a str {
                    &self.ffi_lookup_name.to_str().unwrap()
                }

                pub fn remote<$([<$argp Borrow>]: std::borrow::Borrow<$argp>),*>(
                    &self,
                    actor_id: &ActorID,
                    $($arg: [<$argp Borrow>]),*
                ) -> ObjectRef<R>
                {
                    let mut task_args = Vec::new();
                    $(
                        let result = rmp_serde::to_vec($arg.borrow()).unwrap();
                        task_args.push(result.as_slice());
                    )*
                    ObjectRef::new(
                        ray_rs_sys::internal::submit_task(Some(actor_id), self.ffi_lookup_name.clone(), &task_args)
                    )
                }
            }

            pub struct [<RayActorCreator $n>]<$($argp,)* R> {
                raw_fn: fn($($argty),*) -> R,
                wrapped_fn: ActorCreation,
                ffi_lookup_name: CString,
            }

            // Have this be $argpBorrow: Borrow<argp> instead
            // So that one can accept references as function signature arguments.
            impl<$($argp: serde::Serialize,)* R> [<RayActorCreator $n>]<$($argp,)* R> {
                pub fn new(
                    raw_fn: fn($($argty),*) -> R,
                    wrapped_fn: ActorCreation,
                    ffi_lookup_name: CString,
                ) -> Self {
                    Self { raw_fn, wrapped_fn, ffi_lookup_name }
                }

                pub fn name<'a>(&'a self) -> &'a str {
                    &self.ffi_lookup_name.to_str().unwrap()
                }

                pub fn call(&self, $($arg: $argty),*) -> R {
                    (self.raw_fn)($($arg),*)
                }

                pub fn remote<$([<$argp Borrow>]: std::borrow::Borrow<$argp>),*>(
                    &self,
                    $($arg: [<$argp Borrow>]),*
                ) -> ActorID
                {
                    let mut task_args = Vec::new();
                    $(
                        let result = rmp_serde::to_vec($arg.borrow()).unwrap();
                        task_args.push(result.as_slice());
                    )*
                    ray_rs_sys::internal::create_actor(self.ffi_lookup_name.clone(), &task_args)
                }

                // pub fn ray_call(&self, args: RustBuffer) -> RustBuffer {
                //     (self.wrapped_fn)(args)
                // }
            }

            pub struct [<RayFunction $n>]<$($argp,)* R> {
                raw_fn: fn($($argty),*) -> R,
                wrapped_fn: InvokerFunction,
                ffi_lookup_name: CString,
            }

            // Have this be $argpBorrow: Borrow<argp> instead
            // So that one can accept references as function signature arguments.
            impl<$($argp: serde::Serialize,)* R> [<RayFunction $n>]<$($argp,)* R> {
                pub fn new(
                    raw_fn: fn($($argty),*) -> R,
                    wrapped_fn: InvokerFunction,
                    ffi_lookup_name: CString,
                ) -> Self {
                    Self { raw_fn, wrapped_fn, ffi_lookup_name }
                }

                pub fn name<'a>(&'a self) -> &'a str {
                    &self.ffi_lookup_name.to_str().unwrap()
                }

                pub fn call(&self, $($arg: $argty),*) -> R {
                    (self.raw_fn)($($arg),*)
                }

                pub fn remote<$([<$argp Borrow>]: std::borrow::Borrow<$argp>),*>(
                    &self,
                    $($arg: [<$argp Borrow>]),*
                ) -> ObjectRef<R>
                {
                    let mut task_args = Vec::new();
                    $(
                        let result = rmp_serde::to_vec($arg.borrow()).unwrap();
                        task_args.push(result.as_slice());
                    )*
                    ObjectRef::new(
                        ray_rs_sys::internal::submit_task(None, self.ffi_lookup_name.clone(), &task_args)
                    )
                }

                pub fn create_actor<$([<$argp Borrow>]: std::borrow::Borrow<$argp>),*>(
                    &self,
                    $($arg: [<$argp Borrow>]),*
                ) -> ActorID
                {
                    let mut task_args = Vec::new();
                    $(
                        let result = rmp_serde::to_vec($arg.borrow()).unwrap();
                        task_args.push(result.as_slice());
                    )*
                    // TODO: convert self.ffi_lookup_name.clone() to pass by reference...?
                    ray_rs_sys::internal::create_actor(self.ffi_lookup_name.clone(), &task_args)
                }

                pub fn ray_call(&self, args: RustBuffer) -> RustBuffer {
                    (self.wrapped_fn)(args)
                }

                pub fn get_invoker(&self) -> InvokerFunction {
                    self.wrapped_fn
                }
            }

            #[cfg(nightly)]
            impl<$($argp,)* R> std::ops::FnOnce<($($argty),*)> for [<RayFunction $n>]<$($argp,)* R> {
                type Output = R;
                extern "rust-call" fn call_once(self, ($($arg),*): ($($argty),*)) -> Self::Output {
                    (self.raw_fn)($($arg),*)
                }
            }

            #[cfg(nightly)]
            impl<$($argp,)* R> std::ops::FnMut<($($argty),*)> for [<RayFunction $n>]<$($argp,)* R> {
                extern "rust-call" fn call_mut(&mut self, ($($arg),*): ($($argty),*)) -> Self::Output {
                    (self.raw_fn)($($arg),*)
                }
            }

            #[cfg(nightly)]
            impl<$($argp,)* R> std::ops::Fn<($($argty),*)> for [<RayFunction $n>]<$($argp,)* R> {
                extern "rust-call" fn call(&self, ($($arg),*): ($($argty),*)) -> Self::Output {
                    (self.raw_fn)($($arg),*)
                }
            }
        }
    };
}

impl_ray_function!([0],);
impl_ray_function!([1], a0: T0 [T0]);
impl_ray_function!([2], a0: T0 [T0], a1: T1 [T1]);
impl_ray_function!([3], a0: T0 [T0], a1: T1 [T1], a2: T2 [T2]);
// impl_ray_function!(a0: T0, a1: T1, a2: T2, a3: T3);

#[macro_export]
macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + count!($($xs)*));
}

#[macro_export]
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

#[macro_export]
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
            pub extern "C" fn [<ray_rust_ffi_ $name>](args: RustBuffer) -> RustBuffer {
                let (arg_raw_ptrs, sizes) = rmp_serde::from_read_ref::<_, (Vec<u64>, Vec<u64>)>(
                    &args.destroy_into_vec()
                ).unwrap();
                // TODO: ensure this
                deserialize!(arg_raw_ptrs, sizes, $($arg:$argty),*);
                let ret: $ret = [<ray_rust_private_ $name>]($($arg,)*);
                let result = rmp_serde::to_vec(&ret).unwrap();
                RustBuffer::from_vec(result)
            }
            // #[allow(non_upper_case_globals)]
            lazy_static! {
                pub static ref $name: [<RayFunction $lit_n>]<$($argty,)* $ret>
                    = [<RayFunction $lit_n>]::new(
                        [<ray_rust_private_ $name>],
                        [<ray_rust_ffi_ $name>],
                        CString::new(
                            // concat!(module_path!(), "::", // we are temporarily disabling this
                            // When using proc macros we can modify the extern "C" fn name itself to include the
                            // module path
                            // Else, one could also reasonably mangle for Ray namespace using a random string...
                            stringify!([<ray_rust_ffi_ $name>])
                        ).unwrap(),
                    );
            }

            // Run this function before main
            #[ctor]
            fn [<register_function_name _ray_rust_ffi_ $name>]() {
                let mut guard = GLOBAL_FUNCTION_NAMES_SET.lock().unwrap();
                guard.insert(
                    CString::new(stringify!([<ray_rust_ffi_ $name>])).unwrap()
                );
            }
        }
    };
}

// When porting this to
#[macro_export]
macro_rules! remote_create_actor_internal {
    ($lit_n:literal, $name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block) => {
        paste! {
            fn [<ray_rust_private_ $name>]($($arg: $argty,)*) -> $ret {
                $body
            }

            #[no_mangle]
            pub extern "C" fn [<ray_rust_create_actor_ $name>](args: RustBuffer) -> *mut std::os::raw::c_void {
                let (arg_raw_ptrs, sizes) = rmp_serde::from_read_ref::<_, (Vec<u64>, Vec<u64>)>(
                    &args.destroy_into_vec()
                ).unwrap();
                // TODO: ensure this
                deserialize!(arg_raw_ptrs, sizes $(,$arg:$argty)*);
                let mut ret: Box<$ret> = Box::new([<ray_rust_private_ $name>]($($arg,)*));
                ray_info!("return value: {:?} @ {:p}", ret, &ret);

                let mut ret_owned = std::mem::ManuallyDrop::new(ret);
                let void_ptr = (&mut *(*ret_owned)) as *mut _ as *mut std::os::raw::c_void;
                void_ptr
            }

            // #[allow(non_upper_case_globals)]
            lazy_static! {
                pub static ref $name: [<RayActorCreator $lit_n>]<$($argty,)* $ret>
                    = [<RayActorCreator $lit_n>]::new(
                        [<ray_rust_private_ $name>],
                        [<ray_rust_create_actor_ $name>],
                        CString::new(
                            // concat!(module_path!(), "::", // we are temporarily disabling this
                            // When using proc macros we can modify the extern "C" fn name itself to include the
                            // module path
                            // Else, one could also reasonably mangle for Ray namespace using a random string...
                            stringify!([<ray_rust_create_actor_ $name>])
                        ).unwrap(),
                    );
            }

            // Run this function before main
            #[ctor]
            fn [<register_function_name _ray_rust_ffi_ $name>]() {
                let mut guard = GLOBAL_ACTOR_CREATION_NAMES_SET.lock().unwrap();
                guard.insert(
                    CString::new(stringify!([<ray_rust_create_actor_ $name>])).unwrap()
                );
            }
        }
    };
}

#[macro_export]
macro_rules! strip_ref {
    (&mut t:ty) => {
        t
    };
    (&t:ty) => {
        t
    };
    (t:ty) => {
        t
    };
}

// When porting this to
#[macro_export]
macro_rules! remote_actor_internal {
    ($lit_n:literal, $name:ident ($arg0:ident: $argty0:ty $(,$arg:ident: $argty:ty)*) -> $ret:ty $body:block) => {
        paste! {
            fn [<ray_rust_private_ $name>]($arg0: $argty0 $(,$arg: $argty)*) -> $ret {
                $body
            }

            #[no_mangle]
            // TODO: have the namespace include the class/type's name...
            pub extern "C" fn [<ray_rust_actor_method_ $name>](
                actor_ptr: *mut std::os::raw::c_void,
                args: RustBuffer,
            ) -> RustBuffer {
                let (arg_raw_ptrs, sizes) = rmp_serde::from_read_ref::<_, (Vec<u64>, Vec<u64>)>(
                    &args.destroy_into_vec()
                ).unwrap();
                // TODO: ensure this
                deserialize!(arg_raw_ptrs, sizes $(,$arg:$argty)*);
                let actor_ref = unsafe { &mut *(actor_ptr as *mut _) };

                ray_info!("Actor Value: {:?}", actor_ref);

                let ret: $ret = [<ray_rust_private_ $name>](actor_ref, $($arg,)*);
                let result = rmp_serde::to_vec(&ret).unwrap();
                RustBuffer::from_vec(result)
            }

            // #[allow(non_upper_case_globals)]
            lazy_static! {
                pub static ref $name: [<RayActorMethod $lit_n>]<$($argty,)* $ret>
                    = [<RayActorMethod $lit_n>]::new(
                        CString::new(
                            // concat!(module_path!(), "::", // we are temporarily disabling this
                            // When using proc macros we can modify the extern "C" fn name itself to include the
                            // module path
                            // Else, one could also reasonably mangle for Ray namespace using a random string...
                            stringify!([<ray_rust_actor_method_ $name>])
                        ).unwrap(),
                    );
            }

            // Run this function before main
            #[ctor]
            fn [<register_function_name _ray_rust_ffi_ $name>]() {
                let mut guard = GLOBAL_ACTOR_METHOD_NAMES_SET.lock().unwrap();
                guard.insert(
                    CString::new(stringify!([<ray_rust_actor_method_ $name>])).unwrap()
                );
            }
        }
    };
}

#[macro_export]
macro_rules! match_remote {
    ([$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(0, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(1, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt $y:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(2, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt $y:tt $z:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_internal!(3, $name ($($arg: $argty),*) -> $ret $body); };
    // ($x:tt $y:tt $z: tt $a:tt) => remote_internal!(4, $name($($arg: $argty),*) -> $ret $body);
}

#[macro_export]
macro_rules! match_create_actor_remote {
    ([$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_create_actor_internal!(0, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_create_actor_internal!(1, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt $y:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_create_actor_internal!(2, $name ($($arg: $argty),*) -> $ret $body); };
    ($x:tt $y:tt $z:tt [$name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block]) => { remote_create_actor_internal!(3, $name ($($arg: $argty),*) -> $ret $body); };
    // ($x:tt $y:tt $z: tt $a:tt) => remote_internal!(4, $name($($arg: $argty),*) -> $ret $body);
}

#[macro_export]
macro_rules! match_actor_remote {
    ([$name:ident ($arg0:ident: $argty0:ty $(,$arg:ident: $argty:ty)*) -> $ret:ty $body:block]) => {
        remote_actor_internal!(0, $name ($arg0: $argty0 $(,$arg: $argty)*) -> $ret $body);
    };
    ($x:tt [$name:ident ($arg0:ident: $argty0:ty $(,$arg:ident: $argty:ty)*) -> $ret:ty $body:block]) => {
        remote_actor_internal!(1, $name ($arg0: $argty0 $(,$arg: $argty)*) -> $ret $body);
    };
    ($x:tt $y:tt [$name:ident ($arg0:ident: $argty0:ty $(,$arg:ident: $argty:ty)*) -> $ret:ty $body:block]) => {
        remote_actor_internal!(2, $name ($arg0: $argty0 $(,$arg: $argty)*) -> $ret $body);
    };
    // ($x:tt $y:tt $z: tt $a:tt) => remote_internal!(4, $name($($arg: $argty),*) -> $ret $body);
}

#[macro_export]
macro_rules! remote {
    (pub fn $name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block) => {
        match_remote!($($arg)* [$name ($($arg: $argty),*) -> $ret $body]);
    }
}

#[macro_export]
macro_rules! remote_create_actor {
    (pub fn $name:ident ($($arg:ident: $argty:ty),*) -> $ret:ty $body:block) => {
        match_create_actor_remote!($($arg)* [$name ($($arg: $argty),*) -> $ret $body]);
    }
}

#[macro_export]
macro_rules! remote_actor {
    (pub fn $name:ident ($arg0:ident: $argty0:ty $(,$arg:ident: $argty:ty)*) -> $ret:ty $body:block) => {
        match_actor_remote!($($arg)* [$name ($arg0: $argty0 $(,$arg: $argty),*) -> $ret $body]);
    }
}

pub fn get<R: serde::de::DeserializeOwned>(id: &ObjectRef<R>) -> R {
    let buf = ray_rs_sys::internal::get_slice(id.as_raw(), -1);
    rmp_serde::from_read_ref::<_, R>(buf).unwrap()
}

// #[macro_export]
// macro_rules! remote_actor {
//
// }

// pub fn put<I: serde::Serialize, B: std::borrow::Borrow<I>>(input: B) -> UniquePtr<ray_api_ffi::ObjectID> {
//     let result = rmp_serde::to_vec(input.borrow()).unwrap();
//     ray_rs_sys::put(result)
// }

// unsafe fn allocate_vec_and_copy_from_raw_parts(data: *const u8, size: usize) -> Vec<u8> {
//     let mut vec = Vec::with_capacity(size);
//     let slice = core::slice::from_raw_parts(data, size);
//     vec.copy_from_slice(slice);
//     vec
// }

// struct ObjectRef<T> {
//     _type: PhantomData<T>
// }

// fn main() {
//     use math::vectors::add_two_vecs;
//     add_two_vecs.remote(a, b);
//
// }
