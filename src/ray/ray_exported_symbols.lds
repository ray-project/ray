# This file defines the C++ symbols that need to be exported (aka ABI, application binary interface).
# These symbols will be used by other libraries (e.g., streaming).
# Note: This file is used for macOS only, and should be kept in sync with `ray_version_script.lds`.
# Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
# common
__ZN3ray*Language*
__ZN3ray*RayObject*
__ZN3ray*Status*
__ZN3ray*RayFunction*
__ZN3ray*FunctionDescriptorBuilder*
__ZN3ray*TaskArg*
__ZN3ray*TaskOptions*
__ZN3ray*Buffer*
__ZN3ray*LocalMemoryBuffer*
# util
__ZN3ray*RayLog*
__ZN3ray*RayLogLevel*
# id
__ZN3ray*MurmurHash64A*
__ZN3ray*JobID*
__ZN3ray*TaskID*
__ZN3ray*ActorID*
__ZN3ray*ObjectID*
__ZN3ray*ObjectReference*
# Others
__ZN3ray7metrics*
__ZN3ray5stats*
__ZN3ray3rpc*
__ZN3ray3gcs*
__ZN3ray*CoreWorker*
__ZN3ray*PeriodicalRunner*
__ZN3ray8internal*
*PyInit__raylet*
*Java_io_ray*
_JNI_On*
*aligned_free*
*aligned_malloc*
