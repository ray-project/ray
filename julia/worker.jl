# using CBinding

# c`-std=c++17 -I/home/jonch/Desktop/Programming/systems/ray/julia -lcore_worker_library_c -L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib`
# c"""
# #include "c_worker.h"
# """
# # c"c_worker_InitConfig"(CInt(1), CInt(3), CInt(1), )
# c"c_worker_Initialize"()

using Base.Libc.Libdl

c_worker = dlopen("/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib/libcore_worker_library_c.so")

init_config_sym = dlsym(c_worker, :c_worker_InitConfig)

ccall(init_config_sym, Cvoid,
      (Cint, Cint, Cint, Cstring, Cstring, Cint, Ptr{Ptr{UInt8}}),
      1, 3, 1, "", "--dashboard-port 8266", 1, ["", "--ray_address=192.168.0.96:6385"])

function init()
    ccall(dlsym(c_worker, :c_worker_Initialize), Cvoid, ())
end

function shutdown()
    ccall(dlsym(c_worker, :c_worker_Shutdown), Cvoid, ())
end

init()
shutdown()
dlclose(c_worker)
