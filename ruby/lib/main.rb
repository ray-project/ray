require 'ffi'

module CWorker
  extend FFI::Library
  ffi_lib 'libcore_worker_library_c.so'
  attach_function :c_worker_InitConfig, [:int, :int, :int, :string, :string, :int, :pointer], :void
  attach_function :c_worker_Initialize, [], :void
  attach_function :c_worker_Shutdown, [], :void
end

ptr = FFI::MemoryPointer.new(:pointer, 0)
CWorker.c_worker_InitConfig(1, 3, 1, "", "", 0, ptr)
CWorker.c_worker_Initialize()
CWorker.c_worker_Shutdown()
