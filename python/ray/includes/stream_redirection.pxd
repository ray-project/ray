from libcpp.string cimport string as c_string
from libc.stdint cimport uint64_t
from libcpp cimport bool as c_bool

cdef extern from "ray/util/stream_redirection_options.h" nogil:
    cdef cppclass CStreamRedirectionOptions "ray::StreamRedirectionOption":
        CStreamRedirectionOptions()
        c_string file_path
        uint64_t rotation_max_size
        uint64_t rotation_max_file_count
        c_bool tee_to_stdout
        c_bool tee_to_stderr

cdef extern from "ray/util/stream_redirection.h" namespace "ray" nogil:
    void RedirectStdoutOncePerProcess(const CStreamRedirectionOptions& opt)
    void RedirectStderrOncePerProcess(const CStreamRedirectionOptions& opt)
