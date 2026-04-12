# This file defines the C++ symbols that need to be exported (aka ABI, application binary interface).
# These symbols will be used by other libraries (e.g., streaming).
# Note: This file is used for linux only, and should be kept in sync with `ray_exported_symbols.lds`.
# Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
VERSION_1.0 {
    global:
        # common
        *3ray*Language*;
        *3ray*RayObject*;
        *3ray*Status*;
        *3ray*RayFunction*;
        *3ray*FunctionDescriptorBuilder*;
        *3ray*TaskArg*;
        *3ray*TaskOptions*;
        *3ray*Buffer*;
        *3ray*LocalMemoryBuffer*;
        # util
        *3ray*RayLog*;
        *3ray*RayLogLevel*;
        # id
        *3ray*MurmurHash64A*;
        *3ray*JobID*;
        *3ray*TaskID*;
        *3ray*ActorID*;
        *3ray*ObjectID*;
        *3ray*ObjectReference*;
        # Others
        *3ray*metrics*;
        *3ray*rpc*;
        *3ray*gcs*;
        *3ray*stats*;
        *3ray*CoreWorker*;
        *3ray*PeriodicalRunner*;
        *3ray8internal*;
        *PyInit*;
        *init_raylet*;
        *Java*;
        *JNI_*;
        *aligned_free*;
        *aligned_malloc*;
    local: *;
};
