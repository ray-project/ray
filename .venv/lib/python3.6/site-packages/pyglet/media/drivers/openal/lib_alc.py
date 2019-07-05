# ----------------------------------------------------------------------------
# pyglet
# Copyright (c) 2006-2008 Alex Holkner
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions 
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright 
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#  * Neither the name of pyglet nor the names of its
#    contributors may be used to endorse or promote products
#    derived from this software without specific prior written
#    permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ----------------------------------------------------------------------------
'''Wrapper for openal

Generated with:
../tools/wraptypes/wrap.py /usr/include/AL/alc.h -lopenal -olib_alc.py

.. Hacked to fix ALCvoid argtypes.
'''

__docformat__ =  'restructuredtext'
__version__ = '$Id$'

import ctypes
from ctypes import *
import sys

import pyglet.lib

_lib = pyglet.lib.load_library('openal', win32='openal32',
    framework='/System/Library/Frameworks/OpenAL.framework')

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



ALC_API = 0 	# /usr/include/AL/alc.h:19
ALCAPI = 0 	# /usr/include/AL/alc.h:37
ALC_INVALID = 0 	# /usr/include/AL/alc.h:39
ALC_VERSION_0_1 = 1 	# /usr/include/AL/alc.h:42
class struct_ALCdevice_struct(Structure):
    __slots__ = [
    ]
struct_ALCdevice_struct._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_ALCdevice_struct(Structure):
    __slots__ = [
    ]
struct_ALCdevice_struct._fields_ = [
    ('_opaque_struct', c_int)
]

ALCdevice = struct_ALCdevice_struct 	# /usr/include/AL/alc.h:44
class struct_ALCcontext_struct(Structure):
    __slots__ = [
    ]
struct_ALCcontext_struct._fields_ = [
    ('_opaque_struct', c_int)
]

class struct_ALCcontext_struct(Structure):
    __slots__ = [
    ]
struct_ALCcontext_struct._fields_ = [
    ('_opaque_struct', c_int)
]

ALCcontext = struct_ALCcontext_struct 	# /usr/include/AL/alc.h:45
ALCboolean = c_char 	# /usr/include/AL/alc.h:49
ALCchar = c_char 	# /usr/include/AL/alc.h:52
ALCbyte = c_char 	# /usr/include/AL/alc.h:55
ALCubyte = c_ubyte 	# /usr/include/AL/alc.h:58
ALCshort = c_short 	# /usr/include/AL/alc.h:61
ALCushort = c_ushort 	# /usr/include/AL/alc.h:64
ALCint = c_int 	# /usr/include/AL/alc.h:67
ALCuint = c_uint 	# /usr/include/AL/alc.h:70
ALCsizei = c_int 	# /usr/include/AL/alc.h:73
ALCenum = c_int 	# /usr/include/AL/alc.h:76
ALCfloat = c_float 	# /usr/include/AL/alc.h:79
ALCdouble = c_double 	# /usr/include/AL/alc.h:82
ALCvoid = None 	# /usr/include/AL/alc.h:85
ALC_FALSE = 0 	# /usr/include/AL/alc.h:91
ALC_TRUE = 1 	# /usr/include/AL/alc.h:94
ALC_FREQUENCY = 4103 	# /usr/include/AL/alc.h:99
ALC_REFRESH = 4104 	# /usr/include/AL/alc.h:104
ALC_SYNC = 4105 	# /usr/include/AL/alc.h:109
ALC_MONO_SOURCES = 4112 	# /usr/include/AL/alc.h:114
ALC_STEREO_SOURCES = 4113 	# /usr/include/AL/alc.h:119
ALC_NO_ERROR = 0 	# /usr/include/AL/alc.h:128
ALC_INVALID_DEVICE = 40961 	# /usr/include/AL/alc.h:133
ALC_INVALID_CONTEXT = 40962 	# /usr/include/AL/alc.h:138
ALC_INVALID_ENUM = 40963 	# /usr/include/AL/alc.h:143
ALC_INVALID_VALUE = 40964 	# /usr/include/AL/alc.h:148
ALC_OUT_OF_MEMORY = 40965 	# /usr/include/AL/alc.h:153
ALC_DEFAULT_DEVICE_SPECIFIER = 4100 	# /usr/include/AL/alc.h:159
ALC_DEVICE_SPECIFIER = 4101 	# /usr/include/AL/alc.h:160
ALC_EXTENSIONS = 4102 	# /usr/include/AL/alc.h:161
ALC_MAJOR_VERSION = 4096 	# /usr/include/AL/alc.h:163
ALC_MINOR_VERSION = 4097 	# /usr/include/AL/alc.h:164
ALC_ATTRIBUTES_SIZE = 4098 	# /usr/include/AL/alc.h:166
ALC_ALL_ATTRIBUTES = 4099 	# /usr/include/AL/alc.h:167
ALC_CAPTURE_DEVICE_SPECIFIER = 784 	# /usr/include/AL/alc.h:172
ALC_CAPTURE_DEFAULT_DEVICE_SPECIFIER = 785 	# /usr/include/AL/alc.h:173
ALC_CAPTURE_SAMPLES = 786 	# /usr/include/AL/alc.h:174
# /usr/include/AL/alc.h:180
alcCreateContext = _lib.alcCreateContext
alcCreateContext.restype = POINTER(ALCcontext)
alcCreateContext.argtypes = [POINTER(ALCdevice), POINTER(ALCint)]

# /usr/include/AL/alc.h:182
alcMakeContextCurrent = _lib.alcMakeContextCurrent
alcMakeContextCurrent.restype = ALCboolean
alcMakeContextCurrent.argtypes = [POINTER(ALCcontext)]

# /usr/include/AL/alc.h:184
alcProcessContext = _lib.alcProcessContext
alcProcessContext.restype = None
alcProcessContext.argtypes = [POINTER(ALCcontext)]

# /usr/include/AL/alc.h:186
alcSuspendContext = _lib.alcSuspendContext
alcSuspendContext.restype = None
alcSuspendContext.argtypes = [POINTER(ALCcontext)]

# /usr/include/AL/alc.h:188
alcDestroyContext = _lib.alcDestroyContext
alcDestroyContext.restype = None
alcDestroyContext.argtypes = [POINTER(ALCcontext)]

# /usr/include/AL/alc.h:190
alcGetCurrentContext = _lib.alcGetCurrentContext
alcGetCurrentContext.restype = POINTER(ALCcontext)
alcGetCurrentContext.argtypes = []

# /usr/include/AL/alc.h:192
alcGetContextsDevice = _lib.alcGetContextsDevice
alcGetContextsDevice.restype = POINTER(ALCdevice)
alcGetContextsDevice.argtypes = [POINTER(ALCcontext)]

# /usr/include/AL/alc.h:198
alcOpenDevice = _lib.alcOpenDevice
alcOpenDevice.restype = POINTER(ALCdevice)
alcOpenDevice.argtypes = [POINTER(ALCchar)]

# /usr/include/AL/alc.h:200
alcCloseDevice = _lib.alcCloseDevice
alcCloseDevice.restype = ALCboolean
alcCloseDevice.argtypes = [POINTER(ALCdevice)]

# /usr/include/AL/alc.h:207
alcGetError = _lib.alcGetError
alcGetError.restype = ALCenum
alcGetError.argtypes = [POINTER(ALCdevice)]

# /usr/include/AL/alc.h:215
alcIsExtensionPresent = _lib.alcIsExtensionPresent
alcIsExtensionPresent.restype = ALCboolean
alcIsExtensionPresent.argtypes = [POINTER(ALCdevice), POINTER(ALCchar)]

# /usr/include/AL/alc.h:217
alcGetProcAddress = _lib.alcGetProcAddress
alcGetProcAddress.restype = POINTER(c_void)
alcGetProcAddress.argtypes = [POINTER(ALCdevice), POINTER(ALCchar)]

# /usr/include/AL/alc.h:219
alcGetEnumValue = _lib.alcGetEnumValue
alcGetEnumValue.restype = ALCenum
alcGetEnumValue.argtypes = [POINTER(ALCdevice), POINTER(ALCchar)]

# /usr/include/AL/alc.h:225
alcGetString = _lib.alcGetString
alcGetString.restype = POINTER(ALCchar)
alcGetString.argtypes = [POINTER(ALCdevice), ALCenum]

# /usr/include/AL/alc.h:227
alcGetIntegerv = _lib.alcGetIntegerv
alcGetIntegerv.restype = None
alcGetIntegerv.argtypes = [POINTER(ALCdevice), ALCenum, ALCsizei, POINTER(ALCint)]

# /usr/include/AL/alc.h:233
alcCaptureOpenDevice = _lib.alcCaptureOpenDevice
alcCaptureOpenDevice.restype = POINTER(ALCdevice)
alcCaptureOpenDevice.argtypes = [POINTER(ALCchar), ALCuint, ALCenum, ALCsizei]

# /usr/include/AL/alc.h:235
alcCaptureCloseDevice = _lib.alcCaptureCloseDevice
alcCaptureCloseDevice.restype = ALCboolean
alcCaptureCloseDevice.argtypes = [POINTER(ALCdevice)]

# /usr/include/AL/alc.h:237
alcCaptureStart = _lib.alcCaptureStart
alcCaptureStart.restype = None
alcCaptureStart.argtypes = [POINTER(ALCdevice)]

# /usr/include/AL/alc.h:239
alcCaptureStop = _lib.alcCaptureStop
alcCaptureStop.restype = None
alcCaptureStop.argtypes = [POINTER(ALCdevice)]

# /usr/include/AL/alc.h:241
alcCaptureSamples = _lib.alcCaptureSamples
alcCaptureSamples.restype = None
alcCaptureSamples.argtypes = [POINTER(ALCdevice), POINTER(ALCvoid), ALCsizei]

LPALCCREATECONTEXT = CFUNCTYPE(POINTER(ALCcontext), POINTER(ALCdevice), POINTER(ALCint)) 	# /usr/include/AL/alc.h:246
LPALCMAKECONTEXTCURRENT = CFUNCTYPE(ALCboolean, POINTER(ALCcontext)) 	# /usr/include/AL/alc.h:247
LPALCPROCESSCONTEXT = CFUNCTYPE(None, POINTER(ALCcontext)) 	# /usr/include/AL/alc.h:248
LPALCSUSPENDCONTEXT = CFUNCTYPE(None, POINTER(ALCcontext)) 	# /usr/include/AL/alc.h:249
LPALCDESTROYCONTEXT = CFUNCTYPE(None, POINTER(ALCcontext)) 	# /usr/include/AL/alc.h:250
LPALCGETCURRENTCONTEXT = CFUNCTYPE(POINTER(ALCcontext)) 	# /usr/include/AL/alc.h:251
LPALCGETCONTEXTSDEVICE = CFUNCTYPE(POINTER(ALCdevice), POINTER(ALCcontext)) 	# /usr/include/AL/alc.h:252
LPALCOPENDEVICE = CFUNCTYPE(POINTER(ALCdevice), POINTER(ALCchar)) 	# /usr/include/AL/alc.h:253
LPALCCLOSEDEVICE = CFUNCTYPE(ALCboolean, POINTER(ALCdevice)) 	# /usr/include/AL/alc.h:254
LPALCGETERROR = CFUNCTYPE(ALCenum, POINTER(ALCdevice)) 	# /usr/include/AL/alc.h:255
LPALCISEXTENSIONPRESENT = CFUNCTYPE(ALCboolean, POINTER(ALCdevice), POINTER(ALCchar)) 	# /usr/include/AL/alc.h:256
LPALCGETPROCADDRESS = CFUNCTYPE(POINTER(c_void), POINTER(ALCdevice), POINTER(ALCchar)) 	# /usr/include/AL/alc.h:257
LPALCGETENUMVALUE = CFUNCTYPE(ALCenum, POINTER(ALCdevice), POINTER(ALCchar)) 	# /usr/include/AL/alc.h:258
LPALCGETSTRING = CFUNCTYPE(POINTER(ALCchar), POINTER(ALCdevice), ALCenum) 	# /usr/include/AL/alc.h:259
LPALCGETINTEGERV = CFUNCTYPE(None, POINTER(ALCdevice), ALCenum, ALCsizei, POINTER(ALCint)) 	# /usr/include/AL/alc.h:260
LPALCCAPTUREOPENDEVICE = CFUNCTYPE(POINTER(ALCdevice), POINTER(ALCchar), ALCuint, ALCenum, ALCsizei) 	# /usr/include/AL/alc.h:261
LPALCCAPTURECLOSEDEVICE = CFUNCTYPE(ALCboolean, POINTER(ALCdevice)) 	# /usr/include/AL/alc.h:262
LPALCCAPTURESTART = CFUNCTYPE(None, POINTER(ALCdevice)) 	# /usr/include/AL/alc.h:263
LPALCCAPTURESTOP = CFUNCTYPE(None, POINTER(ALCdevice)) 	# /usr/include/AL/alc.h:264
LPALCCAPTURESAMPLES = CFUNCTYPE(None, POINTER(ALCdevice), POINTER(ALCvoid), ALCsizei) 	# /usr/include/AL/alc.h:265

__all__ = ['ALC_API', 'ALCAPI', 'ALC_INVALID', 'ALC_VERSION_0_1', 'ALCdevice',
'ALCcontext', 'ALCboolean', 'ALCchar', 'ALCbyte', 'ALCubyte', 'ALCshort',
'ALCushort', 'ALCint', 'ALCuint', 'ALCsizei', 'ALCenum', 'ALCfloat',
'ALCdouble', 'ALCvoid', 'ALC_FALSE', 'ALC_TRUE', 'ALC_FREQUENCY',
'ALC_REFRESH', 'ALC_SYNC', 'ALC_MONO_SOURCES', 'ALC_STEREO_SOURCES',
'ALC_NO_ERROR', 'ALC_INVALID_DEVICE', 'ALC_INVALID_CONTEXT',
'ALC_INVALID_ENUM', 'ALC_INVALID_VALUE', 'ALC_OUT_OF_MEMORY',
'ALC_DEFAULT_DEVICE_SPECIFIER', 'ALC_DEVICE_SPECIFIER', 'ALC_EXTENSIONS',
'ALC_MAJOR_VERSION', 'ALC_MINOR_VERSION', 'ALC_ATTRIBUTES_SIZE',
'ALC_ALL_ATTRIBUTES', 'ALC_CAPTURE_DEVICE_SPECIFIER',
'ALC_CAPTURE_DEFAULT_DEVICE_SPECIFIER', 'ALC_CAPTURE_SAMPLES',
'alcCreateContext', 'alcMakeContextCurrent', 'alcProcessContext',
'alcSuspendContext', 'alcDestroyContext', 'alcGetCurrentContext',
'alcGetContextsDevice', 'alcOpenDevice', 'alcCloseDevice', 'alcGetError',
'alcIsExtensionPresent', 'alcGetProcAddress', 'alcGetEnumValue',
'alcGetString', 'alcGetIntegerv', 'alcCaptureOpenDevice',
'alcCaptureCloseDevice', 'alcCaptureStart', 'alcCaptureStop',
'alcCaptureSamples', 'LPALCCREATECONTEXT', 'LPALCMAKECONTEXTCURRENT',
'LPALCPROCESSCONTEXT', 'LPALCSUSPENDCONTEXT', 'LPALCDESTROYCONTEXT',
'LPALCGETCURRENTCONTEXT', 'LPALCGETCONTEXTSDEVICE', 'LPALCOPENDEVICE',
'LPALCCLOSEDEVICE', 'LPALCGETERROR', 'LPALCISEXTENSIONPRESENT',
'LPALCGETPROCADDRESS', 'LPALCGETENUMVALUE', 'LPALCGETSTRING',
'LPALCGETINTEGERV', 'LPALCCAPTUREOPENDEVICE', 'LPALCCAPTURECLOSEDEVICE',
'LPALCCAPTURESTART', 'LPALCCAPTURESTOP', 'LPALCCAPTURESAMPLES']
