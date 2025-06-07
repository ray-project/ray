from libcpp.string cimport string as c_string

cdef extern from "ray/util/setproctitle.h" namespace "ray":
    void setproctitle(const c_string &title)

cpdef python_setproctitle(title: str):
    cdef c_string c_title = title.encode("utf-8")
    setproctitle(c_title)
