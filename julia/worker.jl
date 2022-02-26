# using CBinding

# c`-std=c++17 -I/home/jonch/Desktop/Programming/systems/ray/julia -lcore_worker_library_c -L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib`
# c"""
# #include "c_worker.h"
# """

# # c"c_worker_InitConfig"(CInt(1), CInt(3), CInt(1), )
# c"c_worker_Initialize"()


# c`-std=c++17`
# c"""
# typedef unsigned long long RayInt64;
# typedef RayInt64 RayInt;
# typedef struct RaySlice {
#   // Shouldn't this be `char *data`?
#   // Actually no, we want this to be some arbitrary type
#   // (either a DataBuffer or a DataValue)
#   void *data;
#   RayInt64 len;
#   RayInt64 cap;
# } RaySlice;
#
# // Rename BufferWrapper?
# typedef struct DataBuffer {
#   // TODO: replace with a less-headache-inducing uint64_t here...
#   size_t size;
#   const uint8_t *p;
# } DataBuffer;
#
# // Rename RayValueWrapper?
# typedef struct DataValue {
#   struct DataBuffer *data;
#   struct DataBuffer *meta;
# } DataValue;
# """

# Unfortunately, we can't easily get this data
# directly from the header file...
struct RaySlice
    data::Ptr{Cvoid}
    len::UInt64
    cap::UInt64
end
struct DataBuffer
    size::UInt64
    p::Ptr{UInt8}
end
struct DataValue
    data::Ptr{DataBuffer}
    meta::Ptr{DataBuffer}
end

using Base.Libc.Libdl
using MsgPack

io = IOBuffer(maxsize=4)
pack(io, "hello")
print(io)
print(unpack(seekstart(io)))

c_worker_lib = dlopen(
    "librazor_rs",
    # "libcore_worker_library_c",
)

# TODO: hide these behind a module, and only export select methods
function init_config()
    ccall(
        dlsym(c_worker_lib, :c_worker_InitConfig),
        Cvoid,
        (Cint, Cint, Cint, Cstring, Cstring, Cint, Ptr{Ptr{UInt8}}),
        1,
        3,
        1,
        "",
        "",
        0,
        [], # --ray_address=192.168.0.96:6385
    )
end

function init()
    ccall(dlsym(c_worker_lib, :c_worker_Initialize), Cvoid, ())
end

function shutdown()
    ccall(dlsym(c_worker_lib, :c_worker_Shutdown), Cvoid, ())
    run(`ray stop --force`)
end

function register_callback(f)
    ret = ccall(
        dlsym(c_worker_lib, :c_worker_RegisterExecutionCallback),
        Cint,
        (Ptr{Cvoid},),
        f,
    )
    @assert ret == 1
end

# Passing callbacks to Julia: https://julialang.org/blog/2013/05/callback/
function julia_worker_execute!(
    actor_ptr::Ptr{Ptr{Cvoid}},
    task_type_int::Cint,
    # We might be better off with raw types here? or protobuf types...?
    ray_function_info::RaySlice,
    args::Ptr{Ptr{DataValue}},
    args_len::UInt64,
    return_values::RaySlice,
)::Cvoid

    print("Hello world")
    return Cvoid
end

const julia_worker_execute_c = @cfunction(
    julia_worker_execute!,
    Cvoid,
    (Ptr{Ptr{Cvoid}}, Cint, RaySlice, Ptr{Ptr{DataValue}}, UInt64, RaySlice)
)

register_callback(julia_worker_execute_c)
init_config()
init()
shutdown()

dlclose(c_worker_lib)
