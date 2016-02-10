import os, ctypes

_orchlib_handle = ctypes.CDLL(
   os.path.join(os.path.dirname(os.path.abspath(__file__)), 'liborchlib.so'),
   ctypes.RTLD_GLOBAL
)
