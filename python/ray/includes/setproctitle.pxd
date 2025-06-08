from libcpp.string cimport string as c_string

cdef extern from "ray/util/setproctitle.h" namespace "ray":
    void setproctitle(const c_string &title)
