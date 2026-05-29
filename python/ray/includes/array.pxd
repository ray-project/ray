from libc.stddef cimport size_t
from libcpp.string cimport string

cdef extern from "<array>" namespace "std":
    cdef cppclass array_string_2 "std::array<std::string, 2>":
        string& operator[](size_t) except +
