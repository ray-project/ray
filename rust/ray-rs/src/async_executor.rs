pub use tokio::runtime::{EnterGuard as TokioHandleGuard, Handle as TokioHandle};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use super::{ray_info, lazy_static, RwLock, FiberEvent, TaskData, Mutex, util, LIBRARIES, Arc, rust_worker_execute_async_internal};
use std::{
    cell::RefCell,
    sync::atomic::{AtomicU64, Ordering},
    mem::drop, ops::Drop,
    collections::HashMap,
    marker::PhantomData,
};

use libloading::{Library, Symbol};

lazy_static! {
    static ref TOKIO_HANDLES: RwLock<HashMap<u64, TokioHandle>> = RwLock::new(HashMap::new());
}

static UUID: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "async")]
thread_local! {
    static LOCAL_TOKIO_GUARD: RefCell<Option<TokioHandleGuard<'static>>> = RefCell::new(None)
}

#[no_mangle] pub extern "C" fn tokio_dylib__extern_function____new_handle_uuid() -> u64 {
    UUID.fetch_add(1, Ordering::Relaxed)
}

#[no_mangle] pub extern "C" fn tokio_dylib__extern_function____register_handle(
    h: *mut std::os::raw::c_void,
    uuid: u64
) {
    // This is not quite ffi safe?
    // It requires that TokioHandle has same ABI across main and shared libs
    let mut guard = TOKIO_HANDLES.write().unwrap();
    guard.insert(uuid, unsafe { &*(h as *const TokioHandle) }.clone());
}

#[no_mangle] pub extern "C" fn tokio_dylib__extern_function____on_thread_start(uuid: u64) {
    // Spin until the handle is initiated
    for i in 0..1000 {
        if let Some(handle) = &TOKIO_HANDLES.read().unwrap().get(&uuid) {
            LOCAL_TOKIO_GUARD.with(|ctx| {
                ctx.borrow_mut().replace(unsafe {
                    std::mem::transmute::<_, TokioHandleGuard<'static>>(handle.enter())
                });
            });
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(250));
    }
}

/// Takes the `TokioHandleGuard` from inside the RefCell, dropping it
/// and restoring the shared libs' thread_local tokio::runtime::context::CONTEXT
/// to what it was previously
#[no_mangle] pub extern "C" fn tokio_dylib__extern_function____on_thread_stop() {
    LOCAL_TOKIO_GUARD.with(|ctx| ctx.borrow_mut().take());
}

#[no_mangle] pub extern "C" fn tokio_dylib__extern_function____drop_handle(uuid: u64) {
    assert!(TOKIO_HANDLES.write().unwrap().remove(&uuid).is_some());
}

#[no_mangle] pub extern "C" fn tokio_dylib__extern_function____eprintln_state() {
    eprintln!("UUID: {:?}", UUID);
    eprintln!("TOKIO_HANDLES: {:?}", &TOKIO_HANDLES.read().unwrap());
    LOCAL_TOKIO_GUARD.with(|g| eprintln!("LOCAL_TOKIO_GUARD: {:?}", g));
}

#[derive(Clone)]
struct TokioDylibContext<'a> {
    lib_contexts: Vec<LibraryContext>,
    _phantom_data: PhantomData<&'a [Library]>,
}

#[derive(Clone)]
struct LibraryContext {
    uuid: u64,
    register_handle: Symbol<'static, extern "C" fn(*mut std::os::raw::c_void, u64)>,
    on_thread_start: Symbol<'static, extern "C" fn(u64)>,
    on_thread_stop: Symbol<'static, extern "C" fn()>,
    drop_handle: Symbol<'static, extern "C" fn(u64)>,
    eprintln_state: Symbol<'static, extern "C" fn()>,
}

impl Drop for LibraryContext {
    fn drop(&mut self) {
        let drop_handle = &self.drop_handle;
        let f = &self.eprintln_state;
        f();
        drop_handle(self.uuid);
        f();
        drop(self);
    }
}

macro_rules! get_symbol {
    ($symbol_name:expr, $lib:ident, $sig:ty) => {
        unsafe {
            if let Some(symbol) = $lib.get::<$sig>(
                format!("tokio_dylib__extern_function____{}", $symbol_name).as_bytes()
            ).ok() {
                std::mem::transmute::<Symbol<$sig>, Symbol<'static, $sig>>(symbol)
            } else {
                eprintln!("Unable to find symbol: tokio_dylib__extern_function____{}", $symbol_name);
                break;
            }
        }
    };
}

impl<'a> TokioDylibContext<'a> {
    /// We can only instantiate with a `'static` ref to the library to ensure that TokioDylibContext
    /// has the `'static` lifetime
    fn new(libs: &'static [Library]) -> Self {
        let mut lib_contexts = Vec::with_capacity(libs.len());
        for lib in libs.iter() {
            let new_handle_uuid = get_symbol!("new_handle_uuid", lib, extern "C" fn() -> u64);
            let register_handle = get_symbol!("register_handle", lib, extern "C" fn(*mut std::os::raw::c_void, u64));
            let on_thread_start = get_symbol!("on_thread_start", lib, extern "C" fn(u64));
            let on_thread_stop = get_symbol!("on_thread_stop", lib, extern "C" fn());
            let drop_handle = get_symbol!("drop_handle", lib, extern "C" fn(u64));
            let eprintln_state = get_symbol!("eprintln_state", lib, extern "C" fn());

            let uuid: u64 = new_handle_uuid();
            lib_contexts.push(
                LibraryContext {
                    uuid,
                    register_handle,
                    on_thread_start,
                    on_thread_stop,
                    drop_handle,
                    eprintln_state,
                }
            );
        }
        Self {
            lib_contexts,
            _phantom_data: PhantomData,
        }
    }

    pub fn on_thread_start(&self) {
        for ctx in self.lib_contexts.iter() {
            let on_thread_start = &ctx.on_thread_start;
            on_thread_start(ctx.uuid);
        }
    }

    pub fn on_thread_stop(&self) {
        for ctx in self.lib_contexts.iter() {
            let on_thread_stop = &ctx.on_thread_stop;
            on_thread_stop();
        }
    }

    pub fn eprintln_state(&self) {
        for ctx in self.lib_contexts.iter() {
            let eprintln_state = &ctx.eprintln_state;
            eprintln_state();
        }
    }

    /// Register the tokio handle from the given Tokio library
    fn register_tokio_handle<T: Clone>(&self, handle: &T) {
        let handle_boxed = Box::new(handle.clone());
        let handle_ptr = Box::into_raw(handle_boxed) as *mut std::os::raw::c_void;
        for ctx in self.lib_contexts.iter() {
            let register_handle = &ctx.register_handle;
            register_handle(handle_ptr, ctx.uuid);
        }
    }
}


#[cfg(feature = "async")]
lazy_static! {
    pub(crate) static ref ASYNC_RUNTIME_SENDER:
        Mutex<Option<UnboundedSender<(TaskData, Arc<FiberEvent>)>>> = Mutex::new(None);
}

// Refactor this to perform rt.spawn (to multi-threaded)
// in the boost.asio fiber/thread
#[cfg(feature = "async")]
pub(crate) fn handle_async_startup() {
    let mut guard = ASYNC_RUNTIME_SENDER.lock().unwrap();

    match *guard {
        None => {
            let (tx, mut rx) = unbounded_channel::<(TaskData, Arc<FiberEvent>)>();
            *guard = Some(tx);
            std::thread::spawn(move || {
                let libs = LIBRARIES.read().unwrap();
                // We need to convince the compiler that the lifetime of the libraries
                // from which the symbols are derived exceed that of our async executor
                let libs_ref: &'static _ = unsafe { std::mem::transmute(&libs[..]) };
                // Future: plug-and-play with async-rs etc
                let ctx = TokioDylibContext::new(&libs_ref);

                let ctx_a = ctx.clone();
                let ctx_b = ctx.clone();

                let rt =
                    tokio::runtime::Builder::new_current_thread()
                    // tokio::runtime::Builder::new_multi_thread()
                    // .worker_threads(10)
                    .enable_all()
                    .on_thread_start(move || ctx_a.on_thread_start())
                    .on_thread_stop(move || ctx_b.on_thread_stop())
                    .build()
                    .unwrap();

                ctx.register_tokio_handle(rt.handle());
                ctx.eprintln_state();
                ctx.on_thread_start();
                ctx.eprintln_state();

                ray_info!("rust async executor: looping");
                rt.block_on(async move {
                    loop {
                        let (task_data, notifier) = rx.recv().await.expect("did not receive");
                        tokio::spawn(async move {
                            rust_worker_execute_async_internal(task_data).await;
                            notifier.notify_ready();
                        });
                    }
                });

                eprintln!("Here");
                ctx.on_thread_stop();
                eprintln!("here");
            });
        },
        _ => (),
    };
}
