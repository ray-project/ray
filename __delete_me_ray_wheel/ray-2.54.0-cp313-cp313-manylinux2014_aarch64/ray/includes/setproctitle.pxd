from libcpp.string cimport string as c_string

cdef extern from *:
    """
    extern "C" {
    #include "ray/thirdparty/setproctitle/spt_setup.h"
    }
    """
    int spt_setup()

cdef extern from *:
    """
    extern "C" {
    #include "ray/thirdparty/setproctitle/spt_status.h"
    }
    """
    void set_ps_display(const char *activity, bint force)
