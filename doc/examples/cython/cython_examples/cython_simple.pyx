#!python
# cython: embedsignature=True, binding=True


def simple_func(x, y, z):
    return x + y + z


# Cython code directly callable from Python
def fib(n):
    if n < 2:
        return n
    return fib(n-2) + fib(n-1)


# Typed Cython code
def fib_int(int n):
    if n < 2:
        return n
    return fib_int(n-2) + fib_int(n-1)


# Cython-Python code
cpdef fib_cpdef(int n):
    if n < 2:
        return n
    return fib_cpdef(n-2) + fib_cpdef(n-1)


# C code
def fib_cdef(int n):
    return fib_in_c(n)


cdef int fib_in_c(int n):
    if n < 2:
        return n
    return fib_in_c(n-2) + fib_in_c(n-1)


# Simple class
class simple_class(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value
