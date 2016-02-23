import os, sys, ctypes

MACOSX = (sys.platform in ['darwin'])

_orchlib_handle = ctypes.CDLL(
   os.path.join(os.path.dirname(os.path.abspath(__file__)), 'liborchlib.dylib' if MACOSX else 'liborchlib.so'),
   ctypes.RTLD_GLOBAL
)
