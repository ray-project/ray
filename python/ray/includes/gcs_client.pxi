# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

"""
This PR is a proof of concept that, we *can* do bindings for Async C++ APIs. Specifically, we can wrap a C++ `ray::gcs::GcsClient` Async APIs (by callbacks) into Python Async APIs (by async/await).

## Why?

On Python gRPC, we now have a very complicated and inconsistent way. We have:

- Python `grpcio` based Client.
- Python `grpcio.aio`` based Client and Servers,
- Python `GcsClient` -> C++ `PythonGcsClient` -> C++ `GcsClient`,
- Python `GcsAioClient` -> thread pool executor -> Python `GcsClient` -> C++ `PythonGcsClient` -> C++ `GcsClient`,
- Python `Gcs.*Subscriber` (sync)
- Python `GcsAio.*Subscriber` (async)

All of them talking to the GCS with more or less similar but subtly different APIs. This introduces maintenance overhead, makes debugging harder, and makes it harder to add new features.

Beyond Python, all these APIs are also having slightly different semantics than the C++ GcsClient itself as used by core_worker C++ code. For example, 

1. in _raylet.pyx we liberally added many _auto_reconnect to APIs. This applies to Python GcsClient and GcsAioClient, but not to C++ GcsClient or the Python subscribers. If we tweaked retry counts to "happen to work", it only works for the python code but not core worker code.
2. in `PythonGcsClient::Connect` we retry several times, each time *recreating* a `GcsChannel`. This is supposed to "make reconnection" faster by not waiting in the grpc-internal backoff. But this is not applied in C++ GcsClient or the Python subscribers. In fact, in C++ GcsClient, we don't manage the channel at all. We use the Ray-wide GcsRpcClient to manage it. Indeed, if we wanna "fast recreate" channels, we may want it to be consistenly applied to all clients.
3. in Python GcsClient, we have a method get_all_node_info that forwards the RPC. However we also have a `self._gcs_node_info_stub.GetAllNodeInfo` call in node_head.py, because they want the full reply whereas the Python GcsClient method only returns partial data the original caller wanted.
4. ...and more.

## What's blocking us?

Async. Cython is not known to be good at binding async APIs. We have a few challenges:

1. We need to invoke Python functions in C++ callbacks. This involves a C++ callback class to hold a Python object with all its implications. Today's Ray C++ code base is largely CPython-free.
    1. Cython reference counting. In experimenting I accidentally decreased a PyObject's refcount to -9.
    2. GIL. We need to properly hold and release the locks, requires careful coding and testing.
2. Event loops. In C++ (asio) loop we received the callback, but the python awaiter is waiting in the Python event loop. We need to play with asyncio futures and event loops.
3. (Still risky) Types. C++ callbacks receive C++ Protobuf messages, and Python callbacks receive Python Protobuf messages or Python dicts. We can:
    1. Serialize the C++ Protobuf to string, and deserialize it in Python. This is the approach I chose in this PR.
    2. Bind all C++ protobuf types' fields as .pxd methods (readonly is fine.)


## What's in this PR?

A simple "MyGcsClient" that wraps the C++ GcsClient. It has only 1 method to asynchronously return a "next job id". See this:

```
import ray
from ray._raylet import JobID

ray.init()

x = ray._raylet.my_gcs_client()
import asyncio
async def f():
    wrapper = x.get_next_job_id()
    fut = wrapper.future
    bs = await fut
    job_id = JobID(bs)
    print(job_id)
asyncio.run(f())
```

## What's next?

In P2 (not urgent), we need to evaluate if this is something worth proceeding. We need to answer: (with my current answers)

Q: In endgame, what's benefit? 
A: Removal of all bindings in `##Why?` section above, with a single API consistent with C++ GcsClient.

Q: User visible?
A: No. This is a refactor.

Q: Risk?
A: Types and perf costs. I think the async game is derisked.

Q: Effort, large or small?
A: Large. The binding itself is OK-ish, but there are so many callsites to change.


"""

# This file is a .pxi which is included in _raylet.pyx. This means any already-imported
# symbols can directly be used here. This is not ideal, but we can't easily split this
# out to a separate translation unit because we need to access the singleton
# CCoreWorkerProcess.
#
# We need to best-effort import everything we need.

from ray.includes.common cimport (
    CGcsClient,
    PyBytesCallback,
    PyOptionalIntCallback,
    PyOptionalBytesCallback,
)

cdef class MyGcsClient:
    cdef:
        shared_ptr[CGcsClient] inner
    cdef c_get_next_job_id(self):
        cdef PyBytesCallback cy_callback
        fut, cb = make_future_and_callback()
        cy_callback = PyBytesCallback(cb)
        with nogil:
            check_status(self.inner.get().Jobs().AsyncGetNextJobID(cy_callback))
        return fut
    
    async def get_next_job_id(self):
        result = await self.c_get_next_job_id()
        return JobID(result)

    # Demo: sync method
    def internal_kv_get(self, c_string key, namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            # int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_string value
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKV().Get(ns, key, value)
        if status.IsKeyError():
            return None
        else:
            check_status(status)
            return value

    # Demo: async method
    def c_async_internal_kv_get(self, c_string key, namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            # TODO: timeout is not yet
            PyOptionalBytesCallback cy_callback
        fut, cb = make_future_and_callback()
        cy_callback = PyOptionalBytesCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVGet(ns, key, cy_callback))
        return fut
    async def async_internal_kv_get(self, c_string key, namespace=None, timeout=None):
        status_parts, optional_bytes = await self.c_async_internal_kv_get(key, namespace, timeout)
        check_status(to_c_ray_status(status_parts))
        return optional_bytes

    def c_async_internal_kv_put(self, c_string key, c_string value, c_bool overwrite=False,
                        namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            # TODO: timeout is not yet
            PyOptionalIntCallback cy_callback
        fut, cb = make_future_and_callback()
        cy_callback = PyOptionalIntCallback(cb)
        with nogil:
            check_status(self.inner.get().InternalKV().AsyncInternalKVPut(ns, key, value, overwrite, cy_callback))
        return fut
    async def async_internal_kv_put(self, c_string key, c_string value, c_bool overwrite=False,
                        namespace=None, timeout=None):
        status_parts, optional_int = await self.c_async_internal_kv_put(key, value, overwrite, namespace, timeout)
        check_status(to_c_ray_status(status_parts))
        return optional_int

def make_future_and_callback():
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    def callback(result):
        # May run in in C++ thread
        loop.call_soon_threadsafe(fut.set_result, result)
    return fut, callback

# Now, we serialize ray::Status to 3-tuple, and reconstruct here in Cython. This is not
# necessary: it's used only in cython anyway and you can't use in Python. So to
# optimize, we can use py capsules to store the ray::Status and here to get it back.
cdef CRayStatus to_c_ray_status(tuple):
    cdef:
        uint8_t code = <uint8_t>tuple[0]
        StatusCode status_code = <StatusCode>(code)
        c_string msg = tuple[1]
        int rpc_code = tuple[2]
        CRayStatus s
    if status_code == StatusCode_OK:
        return CRayStatus.OK()
    s = CRayStatus(status_code, msg, rpc_code)
    logger.error(f" msg: {msg}, rpc_code: {rpc_code} in {tuple}")
    logger.error(f" s: {s.ToString()}, {code}, ok: {s.ok()}")
    return s
     


cdef make_my_gcs_client():
    cdef shared_ptr[CGcsClient] inner = CCoreWorkerProcess.GetCoreWorker().GetGcsClient()
    my = MyGcsClient()
    my.inner = inner
    return my

def my_gcs_client():
    return make_my_gcs_client()
