# cython: profile=True
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True

import time
import logging
import os
import sys

from libc.stdint cimport (
    int32_t,
    int64_t,
    INT64_MAX,
    uint64_t,
    uint8_t,
)
from libcpp cimport bool as c_bool
from libcpp.memory cimport (
    dynamic_pointer_cast,
    make_shared,
    shared_ptr,
    unique_ptr,
)
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

cimport cpython

include "includes/native_queue.pxi"
