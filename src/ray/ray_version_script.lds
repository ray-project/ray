# This file defines the C++ symbols that need to be exported (aka ABI, application binary interface).
# These symbols will be used by other libraries (e.g., streaming).
# Note: This file is used for linux only, and should be kept in sync with `ray_exported_symbols.lds`.
# Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
VERSION_1.0 {
    global:
        # common
        _ZN3ray*Language*;
        _ZN3ray*RayObject*;
        _ZN3ray*Status*;
        _ZN3ray*RayFunction*;
        _ZN3ray*FunctionDescriptorBuilder*;
        _ZN3ray*TaskArg*;
        _ZN3ray*TaskOptions*;
        _ZN3ray*Buffer*;
        _ZN3ray*LocalMemoryBuffer*;
        # util
        _ZN3ray*RayLog*;
        _ZN3ray*RayLogLevel*;
        # id
        _ZN3ray*MurmurHash64A*;
        _ZN3ray*JobID*;
        _ZN3ray*TaskID*;
        _ZN3ray*ActorID*;
        _ZN3ray*ObjectID*;
        _ZN3ray*ObjectReference*;
        # Others
        _ZN3ray7metrics*;
        _ZN3ray5rpc*;
        _ZN3ray3gcs*;
        _ZN3ray3stats*;
        _ZN3ray*CoreWorker*;
        _ZN3ray*PeriodicalRunner*;
        _ZN3ray8internal*;
        *PyInit*;
        *init_raylet*;
        *Java*;
        _JNI_On*;
        *aligned_free*;
        *aligned_malloc*;
    local: *;
};
