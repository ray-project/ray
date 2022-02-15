pub use tokio::runtime::{EnterGuard as TokioHandleGuard, Handle as TokioHandle};
use super::{Symbol, ray_info, lazy_static, RwLock, FiberEvent, TaskData, Mutex, util, LIBRARIES, Arc, rust_worker_execute_async_internal};
use std::{
    cell::RefCell,
};

#[cfg(feature = "async")]
lazy_static! {
    pub(crate) static ref ASYNC_RUNTIME_SENDER: Mutex<Option<tokio::sync::mpsc::UnboundedSender<(TaskData, Arc<FiberEvent>)>>> =
        Mutex::new(None);
    static ref TOKIO_HANDLE: RwLock<Option<TokioHandle>> = RwLock::new(None);
}

#[cfg(feature = "async")]
thread_local! {
    static LOCAL_TOKIO_GUARD: RefCell<Option<TokioHandleGuard<'static>>> = RefCell::new(None)
}

#[cfg(feature = "async")]
#[no_mangle]
pub extern "C" fn ray_rust_async__tokio_store_handle(h: *mut std::os::raw::c_void) {
    // This is not quite ffi safe?
    // It requires that TokioHandle has same ABI across main and shared libs
    let mut guard = TOKIO_HANDLE.write().unwrap();
    *guard = Some(unsafe { &*(h as *const TokioHandle) }.clone());
}

#[cfg(feature = "async")]
#[no_mangle]
pub extern "C" fn ray_rust_async__tokio_on_thread_start() {
    // Spin until the handle is initiated
    for i in 0..1000 {
        if let Some(handle) = &*TOKIO_HANDLE.read().unwrap() {
            LOCAL_TOKIO_GUARD.with(|ctx| {
                ctx.borrow_mut().replace(unsafe {
                    std::mem::transmute::<_, TokioHandleGuard<'static>>(handle.enter())
                });
            });
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(200));
    }
}

/// Takes the `TokioHandleGuard` from inside the RefCell, dropping it
/// and restoring the shared libs' thread_local tokio::runtime::context::CONTEXT
/// to what it was previously
#[cfg(feature = "async")]
#[no_mangle]
pub extern "C" fn ray_rust_async__tokio_on_thread_stop() {
    LOCAL_TOKIO_GUARD.with(|ctx| ctx.borrow_mut().take());
}

fn get_tokio_handle_callbacks() -> (Vec<Symbol<'static, extern "C" fn()>>, Vec<Symbol<'static, extern "C" fn()>>) {
    let mut on_thread_start = vec![];
    let mut on_thread_stop = vec![];

    for lib in LIBRARIES.read().unwrap().iter() {
        // TODO maybe also perform error handling for invocation
        unsafe {
            lib.get::<extern "C" fn()>("ray_rust_async__tokio_on_thread_start".as_bytes()).ok()
                .and_then(|symbol| {
                    let static_symbol = unsafe { std::mem::transmute::<Symbol<_>, Symbol<'static, _>>(symbol) };
                    Some(on_thread_start.push(static_symbol))
                });

            lib.get::<extern "C" fn()>("ray_rust_async__tokio_on_thread_stop".as_bytes()).ok()
                .and_then(|symbol| {
                    let static_symbol = unsafe { std::mem::transmute::<Symbol<_>, Symbol<'static, _>>(symbol) };
                    Some(on_thread_stop.push(static_symbol))
                });
        }
    }

    (on_thread_start, on_thread_stop)
}
// Refactor this to perform rt.spawn (to multi-threaded)
// in the boost.asio fiber/thread
#[cfg(feature = "async")]
pub(crate) fn handle_async_startup() {
    let mut guard = ASYNC_RUNTIME_SENDER.lock().unwrap();

    match *guard {
        // TODO: do proper error handling here.
        // TODO: extend to rt-multi-thread (e.g. Runtime::new())
        //
        // Shouldn't this be spawned on another thread, though, so as not
        // to block the current one...?
        //
        // Tokio futures need to be Send anyway... Except if you are using
        // tokio::task::LocalSet??
        //
        // Idea: get rid of
        None => {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(TaskData, Arc<FiberEvent>)>();
            *guard = Some(tx);
            std::thread::spawn(move || {
                // Future: plug-and-play with async-rs etc
                let (on_start, on_stop) = get_tokio_handle_callbacks();

                let rt =
                    tokio::runtime::Builder::new_current_thread()
                    // tokio::runtime::Builder::new_multi_thread()
                    // .worker_threads(1)
                    .enable_all()
                    .on_thread_start(move || {
                        for c in on_start.iter() {
                            c()
                        }
                    })
                    .on_thread_stop(move || {
                        for c in on_stop.iter() {
                            c()
                        }
                    })
                    .build()
                    .unwrap();

                let handle_boxed = Box::new(rt.handle().clone());
                let handle_ptr = Box::into_raw(handle_boxed) as *mut std::os::raw::c_void;
                for lib in LIBRARIES.read().unwrap().iter() {
                    // TODO maybe also perform error handling for invocation
                    unsafe {
                        lib.get::<extern "C" fn(*const std::os::raw::c_void)>(
                            "ray_rust_async__tokio_store_handle".as_bytes()
                        ).ok()
                        .and_then(|symbol| {
                            ray_info!("Registering Handle to shared lib's tokio thread_local Handle");
                            Some(symbol(handle_ptr))
                        });
                    }
                }

                ray_info!("Looping");
                rt.block_on(async move {
                    loop {
                        let (task_data, notifier) = rx.recv().await.expect("did not receive");
                        tokio::spawn(async move {
                            // tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            rust_worker_execute_async_internal(task_data).await;
                            notifier.notify_ready();
                        });
                    }
                });
            });
        },
        _ => (),
    };
}
