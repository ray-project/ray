# This file defines the C++ symbols that need to be exported (aka ABI, application binary interface).
# These symbols will be used by other libraries (e.g., streaming).
# Note: This file is used for macOS only, and should be kept in sync with `ray_version_script.lds`.
# Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
# common
*ray*Language*
*ray*RayObject*
*ray*Status*
*ray*RayFunction*
*ray*FunctionDescriptorBuilder*
*ray*TaskArg*
*ray*TaskOptions*
*ray*Buffer*
*ray*LocalMemoryBuffer*
# util
*ray*RayLog*
*ray*RayLogLevel*
# id
*ray*MurmurHash64A*
*ray*JobID*
*ray*TaskID*
*ray*ActorID*
*ray*ObjectID*
*ray*ObjectReference*
# Others
*ray*metrics*
*ray*stats*
*ray*rpc*
*ray*gcs*
*ray*CoreWorker*
*ray*PeriodicalRunner*
*ray*internal*
*PyInit__raylet*
*Java_io_ray*
_JNI_On*
