'''Wrapper for Xxf86vm

Generated with:
tools/genwrappers.py xf86vmode

Do not modify this file.
'''

__docformat__ =  'restructuredtext'
__version__ = '$Id$'

import ctypes
from ctypes import *

import pyglet.lib

_lib = pyglet.lib.load_library('Xxf86vm')

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

X_XF86VidModeQueryVersion = 0 	# /usr/include/X11/extensions/xf86vmode.h:4885
X_XF86VidModeGetModeLine = 1 	# /usr/include/X11/extensions/xf86vmode.h:4886
X_XF86VidModeModModeLine = 2 	# /usr/include/X11/extensions/xf86vmode.h:4887
X_XF86VidModeSwitchMode = 3 	# /usr/include/X11/extensions/xf86vmode.h:4888
X_XF86VidModeGetMonitor = 4 	# /usr/include/X11/extensions/xf86vmode.h:4889
X_XF86VidModeLockModeSwitch = 5 	# /usr/include/X11/extensions/xf86vmode.h:4890
X_XF86VidModeGetAllModeLines = 6 	# /usr/include/X11/extensions/xf86vmode.h:4891
X_XF86VidModeAddModeLine = 7 	# /usr/include/X11/extensions/xf86vmode.h:4892
X_XF86VidModeDeleteModeLine = 8 	# /usr/include/X11/extensions/xf86vmode.h:4893
X_XF86VidModeValidateModeLine = 9 	# /usr/include/X11/extensions/xf86vmode.h:4894
X_XF86VidModeSwitchToMode = 10 	# /usr/include/X11/extensions/xf86vmode.h:4895
X_XF86VidModeGetViewPort = 11 	# /usr/include/X11/extensions/xf86vmode.h:4896
X_XF86VidModeSetViewPort = 12 	# /usr/include/X11/extensions/xf86vmode.h:4897
X_XF86VidModeGetDotClocks = 13 	# /usr/include/X11/extensions/xf86vmode.h:4899
X_XF86VidModeSetClientVersion = 14 	# /usr/include/X11/extensions/xf86vmode.h:4900
X_XF86VidModeSetGamma = 15 	# /usr/include/X11/extensions/xf86vmode.h:4901
X_XF86VidModeGetGamma = 16 	# /usr/include/X11/extensions/xf86vmode.h:4902
X_XF86VidModeGetGammaRamp = 17 	# /usr/include/X11/extensions/xf86vmode.h:4903
X_XF86VidModeSetGammaRamp = 18 	# /usr/include/X11/extensions/xf86vmode.h:4904
X_XF86VidModeGetGammaRampSize = 19 	# /usr/include/X11/extensions/xf86vmode.h:4905
X_XF86VidModeGetPermissions = 20 	# /usr/include/X11/extensions/xf86vmode.h:4906
CLKFLAG_PROGRAMABLE = 1 	# /usr/include/X11/extensions/xf86vmode.h:4908
XF86VidModeNumberEvents = 0 	# /usr/include/X11/extensions/xf86vmode.h:4919
XF86VidModeBadClock = 0 	# /usr/include/X11/extensions/xf86vmode.h:4922
XF86VidModeBadHTimings = 1 	# /usr/include/X11/extensions/xf86vmode.h:4923
XF86VidModeBadVTimings = 2 	# /usr/include/X11/extensions/xf86vmode.h:4924
XF86VidModeModeUnsuitable = 3 	# /usr/include/X11/extensions/xf86vmode.h:4925
XF86VidModeExtensionDisabled = 4 	# /usr/include/X11/extensions/xf86vmode.h:4926
XF86VidModeClientNotLocal = 5 	# /usr/include/X11/extensions/xf86vmode.h:4927
XF86VidModeZoomLocked = 6 	# /usr/include/X11/extensions/xf86vmode.h:4928
XF86VidModeNumberErrors = 7 	# /usr/include/X11/extensions/xf86vmode.h:4929
XF86VM_READ_PERMISSION = 1 	# /usr/include/X11/extensions/xf86vmode.h:4931
XF86VM_WRITE_PERMISSION = 2 	# /usr/include/X11/extensions/xf86vmode.h:4932
class struct_anon_93(Structure):
    __slots__ = [
        'hdisplay',
        'hsyncstart',
        'hsyncend',
        'htotal',
        'hskew',
        'vdisplay',
        'vsyncstart',
        'vsyncend',
        'vtotal',
        'flags',
        'privsize',
        'private',
    ]
INT32 = c_int 	# /usr/include/X11/Xmd.h:135
struct_anon_93._fields_ = [
    ('hdisplay', c_ushort),
    ('hsyncstart', c_ushort),
    ('hsyncend', c_ushort),
    ('htotal', c_ushort),
    ('hskew', c_ushort),
    ('vdisplay', c_ushort),
    ('vsyncstart', c_ushort),
    ('vsyncend', c_ushort),
    ('vtotal', c_ushort),
    ('flags', c_uint),
    ('privsize', c_int),
    ('private', POINTER(INT32)),
]

XF86VidModeModeLine = struct_anon_93 	# /usr/include/X11/extensions/xf86vmode.h:4954
class struct_anon_94(Structure):
    __slots__ = [
        'dotclock',
        'hdisplay',
        'hsyncstart',
        'hsyncend',
        'htotal',
        'hskew',
        'vdisplay',
        'vsyncstart',
        'vsyncend',
        'vtotal',
        'flags',
        'privsize',
        'private',
    ]
struct_anon_94._fields_ = [
    ('dotclock', c_uint),
    ('hdisplay', c_ushort),
    ('hsyncstart', c_ushort),
    ('hsyncend', c_ushort),
    ('htotal', c_ushort),
    ('hskew', c_ushort),
    ('vdisplay', c_ushort),
    ('vsyncstart', c_ushort),
    ('vsyncend', c_ushort),
    ('vtotal', c_ushort),
    ('flags', c_uint),
    ('privsize', c_int),
    ('private', POINTER(INT32)),
]

XF86VidModeModeInfo = struct_anon_94 	# /usr/include/X11/extensions/xf86vmode.h:4975
class struct_anon_95(Structure):
    __slots__ = [
        'hi',
        'lo',
    ]
struct_anon_95._fields_ = [
    ('hi', c_float),
    ('lo', c_float),
]

XF86VidModeSyncRange = struct_anon_95 	# /usr/include/X11/extensions/xf86vmode.h:4980
class struct_anon_96(Structure):
    __slots__ = [
        'vendor',
        'model',
        'EMPTY',
        'nhsync',
        'hsync',
        'nvsync',
        'vsync',
    ]
struct_anon_96._fields_ = [
    ('vendor', c_char_p),
    ('model', c_char_p),
    ('EMPTY', c_float),
    ('nhsync', c_ubyte),
    ('hsync', POINTER(XF86VidModeSyncRange)),
    ('nvsync', c_ubyte),
    ('vsync', POINTER(XF86VidModeSyncRange)),
]

XF86VidModeMonitor = struct_anon_96 	# /usr/include/X11/extensions/xf86vmode.h:4990
class struct_anon_97(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'root',
        'state',
        'kind',
        'forced',
        'time',
    ]
Display = pyglet.libs.x11.xlib.Display
Window = pyglet.libs.x11.xlib.Window
Time = pyglet.libs.x11.xlib.Time
struct_anon_97._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('root', Window),
    ('state', c_int),
    ('kind', c_int),
    ('forced', c_int),
    ('time', Time),
]

XF86VidModeNotifyEvent = struct_anon_97 	# /usr/include/X11/extensions/xf86vmode.h:5002
class struct_anon_98(Structure):
    __slots__ = [
        'red',
        'green',
        'blue',
    ]
struct_anon_98._fields_ = [
    ('red', c_float),
    ('green', c_float),
    ('blue', c_float),
]

XF86VidModeGamma = struct_anon_98 	# /usr/include/X11/extensions/xf86vmode.h:5008
# /usr/include/X11/extensions/xf86vmode.h:5018
XF86VidModeQueryVersion = _lib.XF86VidModeQueryVersion
XF86VidModeQueryVersion.restype = c_int
XF86VidModeQueryVersion.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/xf86vmode.h:5024
XF86VidModeQueryExtension = _lib.XF86VidModeQueryExtension
XF86VidModeQueryExtension.restype = c_int
XF86VidModeQueryExtension.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/xf86vmode.h:5030
XF86VidModeSetClientVersion = _lib.XF86VidModeSetClientVersion
XF86VidModeSetClientVersion.restype = c_int
XF86VidModeSetClientVersion.argtypes = [POINTER(Display)]

# /usr/include/X11/extensions/xf86vmode.h:5034
XF86VidModeGetModeLine = _lib.XF86VidModeGetModeLine
XF86VidModeGetModeLine.restype = c_int
XF86VidModeGetModeLine.argtypes = [POINTER(Display), c_int, POINTER(c_int), POINTER(XF86VidModeModeLine)]

# /usr/include/X11/extensions/xf86vmode.h:5041
XF86VidModeGetAllModeLines = _lib.XF86VidModeGetAllModeLines
XF86VidModeGetAllModeLines.restype = c_int
XF86VidModeGetAllModeLines.argtypes = [POINTER(Display), c_int, POINTER(c_int), POINTER(POINTER(POINTER(XF86VidModeModeInfo)))]

# /usr/include/X11/extensions/xf86vmode.h:5048
XF86VidModeAddModeLine = _lib.XF86VidModeAddModeLine
XF86VidModeAddModeLine.restype = c_int
XF86VidModeAddModeLine.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeModeInfo), POINTER(XF86VidModeModeInfo)]

# /usr/include/X11/extensions/xf86vmode.h:5055
XF86VidModeDeleteModeLine = _lib.XF86VidModeDeleteModeLine
XF86VidModeDeleteModeLine.restype = c_int
XF86VidModeDeleteModeLine.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeModeInfo)]

# /usr/include/X11/extensions/xf86vmode.h:5061
XF86VidModeModModeLine = _lib.XF86VidModeModModeLine
XF86VidModeModModeLine.restype = c_int
XF86VidModeModModeLine.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeModeLine)]

# /usr/include/X11/extensions/xf86vmode.h:5067
XF86VidModeValidateModeLine = _lib.XF86VidModeValidateModeLine
XF86VidModeValidateModeLine.restype = c_int
XF86VidModeValidateModeLine.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeModeInfo)]

# /usr/include/X11/extensions/xf86vmode.h:5073
XF86VidModeSwitchMode = _lib.XF86VidModeSwitchMode
XF86VidModeSwitchMode.restype = c_int
XF86VidModeSwitchMode.argtypes = [POINTER(Display), c_int, c_int]

# /usr/include/X11/extensions/xf86vmode.h:5079
XF86VidModeSwitchToMode = _lib.XF86VidModeSwitchToMode
XF86VidModeSwitchToMode.restype = c_int
XF86VidModeSwitchToMode.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeModeInfo)]

# /usr/include/X11/extensions/xf86vmode.h:5085
XF86VidModeLockModeSwitch = _lib.XF86VidModeLockModeSwitch
XF86VidModeLockModeSwitch.restype = c_int
XF86VidModeLockModeSwitch.argtypes = [POINTER(Display), c_int, c_int]

# /usr/include/X11/extensions/xf86vmode.h:5091
XF86VidModeGetMonitor = _lib.XF86VidModeGetMonitor
XF86VidModeGetMonitor.restype = c_int
XF86VidModeGetMonitor.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeMonitor)]

# /usr/include/X11/extensions/xf86vmode.h:5097
XF86VidModeGetViewPort = _lib.XF86VidModeGetViewPort
XF86VidModeGetViewPort.restype = c_int
XF86VidModeGetViewPort.argtypes = [POINTER(Display), c_int, POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/xf86vmode.h:5104
XF86VidModeSetViewPort = _lib.XF86VidModeSetViewPort
XF86VidModeSetViewPort.restype = c_int
XF86VidModeSetViewPort.argtypes = [POINTER(Display), c_int, c_int, c_int]

# /usr/include/X11/extensions/xf86vmode.h:5111
XF86VidModeGetDotClocks = _lib.XF86VidModeGetDotClocks
XF86VidModeGetDotClocks.restype = c_int
XF86VidModeGetDotClocks.argtypes = [POINTER(Display), c_int, POINTER(c_int), POINTER(c_int), POINTER(c_int), POINTER(POINTER(c_int))]

# /usr/include/X11/extensions/xf86vmode.h:5120
XF86VidModeGetGamma = _lib.XF86VidModeGetGamma
XF86VidModeGetGamma.restype = c_int
XF86VidModeGetGamma.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeGamma)]

# /usr/include/X11/extensions/xf86vmode.h:5126
XF86VidModeSetGamma = _lib.XF86VidModeSetGamma
XF86VidModeSetGamma.restype = c_int
XF86VidModeSetGamma.argtypes = [POINTER(Display), c_int, POINTER(XF86VidModeGamma)]

# /usr/include/X11/extensions/xf86vmode.h:5132
XF86VidModeSetGammaRamp = _lib.XF86VidModeSetGammaRamp
XF86VidModeSetGammaRamp.restype = c_int
XF86VidModeSetGammaRamp.argtypes = [POINTER(Display), c_int, c_int, POINTER(c_ushort), POINTER(c_ushort), POINTER(c_ushort)]

# /usr/include/X11/extensions/xf86vmode.h:5141
XF86VidModeGetGammaRamp = _lib.XF86VidModeGetGammaRamp
XF86VidModeGetGammaRamp.restype = c_int
XF86VidModeGetGammaRamp.argtypes = [POINTER(Display), c_int, c_int, POINTER(c_ushort), POINTER(c_ushort), POINTER(c_ushort)]

# /usr/include/X11/extensions/xf86vmode.h:5150
XF86VidModeGetGammaRampSize = _lib.XF86VidModeGetGammaRampSize
XF86VidModeGetGammaRampSize.restype = c_int
XF86VidModeGetGammaRampSize.argtypes = [POINTER(Display), c_int, POINTER(c_int)]

# /usr/include/X11/extensions/xf86vmode.h:5156
XF86VidModeGetPermissions = _lib.XF86VidModeGetPermissions
XF86VidModeGetPermissions.restype = c_int
XF86VidModeGetPermissions.argtypes = [POINTER(Display), c_int, POINTER(c_int)]


__all__ = ['X_XF86VidModeQueryVersion', 'X_XF86VidModeGetModeLine',
'X_XF86VidModeModModeLine', 'X_XF86VidModeSwitchMode',
'X_XF86VidModeGetMonitor', 'X_XF86VidModeLockModeSwitch',
'X_XF86VidModeGetAllModeLines', 'X_XF86VidModeAddModeLine',
'X_XF86VidModeDeleteModeLine', 'X_XF86VidModeValidateModeLine',
'X_XF86VidModeSwitchToMode', 'X_XF86VidModeGetViewPort',
'X_XF86VidModeSetViewPort', 'X_XF86VidModeGetDotClocks',
'X_XF86VidModeSetClientVersion', 'X_XF86VidModeSetGamma',
'X_XF86VidModeGetGamma', 'X_XF86VidModeGetGammaRamp',
'X_XF86VidModeSetGammaRamp', 'X_XF86VidModeGetGammaRampSize',
'X_XF86VidModeGetPermissions', 'CLKFLAG_PROGRAMABLE',
'XF86VidModeNumberEvents', 'XF86VidModeBadClock', 'XF86VidModeBadHTimings',
'XF86VidModeBadVTimings', 'XF86VidModeModeUnsuitable',
'XF86VidModeExtensionDisabled', 'XF86VidModeClientNotLocal',
'XF86VidModeZoomLocked', 'XF86VidModeNumberErrors', 'XF86VM_READ_PERMISSION',
'XF86VM_WRITE_PERMISSION', 'XF86VidModeModeLine', 'XF86VidModeModeInfo',
'XF86VidModeSyncRange', 'XF86VidModeMonitor', 'XF86VidModeNotifyEvent',
'XF86VidModeGamma', 'XF86VidModeQueryVersion', 'XF86VidModeQueryExtension',
'XF86VidModeSetClientVersion', 'XF86VidModeGetModeLine',
'XF86VidModeGetAllModeLines', 'XF86VidModeAddModeLine',
'XF86VidModeDeleteModeLine', 'XF86VidModeModModeLine',
'XF86VidModeValidateModeLine', 'XF86VidModeSwitchMode',
'XF86VidModeSwitchToMode', 'XF86VidModeLockModeSwitch',
'XF86VidModeGetMonitor', 'XF86VidModeGetViewPort', 'XF86VidModeSetViewPort',
'XF86VidModeGetDotClocks', 'XF86VidModeGetGamma', 'XF86VidModeSetGamma',
'XF86VidModeSetGammaRamp', 'XF86VidModeGetGammaRamp',
'XF86VidModeGetGammaRampSize', 'XF86VidModeGetPermissions']
