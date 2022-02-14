# flake8: noqa
import numpy
import pyximport

pyximport.install(setup_args={"include_dirs": numpy.get_include()})

from .cython_simple import simple_func, fib, fib_int, fib_cpdef, fib_cdef, simple_class
from .masked_log import masked_log

from .cython_blas import (
    compute_self_corr_for_voxel_sel,
    compute_kernel_matrix,
    compute_single_self_corr_syrk,
    compute_single_self_corr_gemm,
    compute_corr_vectors,
    compute_single_matrix_multiplication,
)

__all__ = [
    "simple_func",
    "fib",
    "fib_int",
    "fib_cpdef",
    "fib_cdef",
    "simple_class",
    "masked_log",
    "compute_self_corr_for_voxel_sel",
    "compute_kernel_matrix",
    "compute_single_self_corr_syrk",
    "compute_single_self_corr_gemm",
    "compute_corr_vectors",
    "compute_single_matrix_multiplication",
]
