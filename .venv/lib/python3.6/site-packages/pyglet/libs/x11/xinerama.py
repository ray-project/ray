'''Wrapper for Xinerama

Generated with:
tools/genwrappers.py xinerama

Do not modify this file.
'''

__docformat__ =  'restructuredtext'
__version__ = '$Id$'

import ctypes
from ctypes import *

import pyglet.lib

_lib = pyglet.lib.load_library('Xinerama')

_int_types = (c_int16, c_int32)
if hasattr(ctypes, 'c_int64'):
    # Some builds of ctypes apparently do not have c_int64
    # defined; it's a pretty good bet that these builds do not
    # have 64-bit pointers.
    _int_types += (ctypes.c_int64,)
for t in _int_types:
    if sizeof(t) == sizeof(c_size_t):
        c_ptrdiff_t = t

class c_void(Structure):
    # c_void_p is a buggy return type, converting to int, so
    # POINTER(None) == c_void_p is actually written as
    # POINTER(c_void), so it can be treated as a real pointer.
    _fields_ = [('dummy', c_int)]


import pyglet.libs.x11.xlib

class struct_anon_93(Structure):
    __slots__ = [
        'screen_number',
        'x_org',
        'y_org',
        'width',
        'height',
    ]
struct_anon_93._fields_ = [
    ('screen_number', c_int),
    ('x_org', c_short),
    ('y_org', c_short),
    ('width', c_short),
    ('height', c_short),
]

XineramaScreenInfo = struct_anon_93 	# /usr/include/X11/extensions/Xinerama.h:40
Display = pyglet.libs.x11.xlib.Display
# /usr/include/X11/extensions/Xinerama.h:44
XineramaQueryExtension = _lib.XineramaQueryExtension
XineramaQueryExtension.restype = c_int
XineramaQueryExtension.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/Xinerama.h:50
XineramaQueryVersion = _lib.XineramaQueryVersion
XineramaQueryVersion.restype = c_int
XineramaQueryVersion.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/Xinerama.h:56
XineramaIsActive = _lib.XineramaIsActive
XineramaIsActive.restype = c_int
XineramaIsActive.argtypes = [POINTER(Display)]

# /usr/include/X11/extensions/Xinerama.h:67
XineramaQueryScreens = _lib.XineramaQueryScreens
XineramaQueryScreens.restype = POINTER(XineramaScreenInfo)
XineramaQueryScreens.argtypes = [POINTER(Display), POINTER(c_int)]


__all__ = ['XineramaScreenInfo', 'XineramaQueryExtension',
'XineramaQueryVersion', 'XineramaIsActive', 'XineramaQueryScreens']
