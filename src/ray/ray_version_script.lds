# Note: Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
VERSION_1.0 {
    global:
        # common
        *ray*Language*;
        *ray*RayObject*;
        *ray*Status*;
        *ray*RayFunction*;
        *ray*TaskArg*;
        *ray*TaskOptions*;
        *ray*Buffer*;
        *ray*LocalMemoryBuffer*;
        # util
        *ray*RayLog*;
        *ray*RayLogLevel*;
        # id
        *ray*MurmurHash64A*;
        *ray*JobID*;
        *ray*TaskID*;
        *ray*ActorID*;
        *ray*ObjectID*;
        # Others
        *ray*CoreWorker*;
        *PyInit*;
        *Java*;
    local: *;
};
