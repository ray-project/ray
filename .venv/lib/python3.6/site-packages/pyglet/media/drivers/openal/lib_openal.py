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
../tools/wraptypes/wrap.py /usr/include/AL/al.h -lopenal -olib_openal.py

.. Hacked to remove non-existent library functions.

TODO add alGetError check.

.. alListener3i and alListeneriv are present in my OS X 10.4 but not another
10.4 user's installation.  They've also been removed for compatibility.
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



AL_API = 0 	# /usr/include/AL/al.h:39
ALAPI = 0 	# /usr/include/AL/al.h:59
AL_INVALID = -1 	# /usr/include/AL/al.h:61
AL_ILLEGAL_ENUM = 0 	# /usr/include/AL/al.h:62
AL_ILLEGAL_COMMAND = 0 	# /usr/include/AL/al.h:63
ALboolean = c_int 	# Better return type than c_char, as generated
ALchar = c_char 	# /usr/include/AL/al.h:73
ALbyte = c_char 	# /usr/include/AL/al.h:76
ALubyte = c_ubyte 	# /usr/include/AL/al.h:79
ALshort = c_short 	# /usr/include/AL/al.h:82
ALushort = c_ushort 	# /usr/include/AL/al.h:85
ALint = c_int 	# /usr/include/AL/al.h:88
ALuint = c_uint 	# /usr/include/AL/al.h:91
ALsizei = c_int 	# /usr/include/AL/al.h:94
ALenum = c_int 	# /usr/include/AL/al.h:97
ALfloat = c_float 	# /usr/include/AL/al.h:100
ALdouble = c_double 	# /usr/include/AL/al.h:103
ALvoid = None 	# /usr/include/AL/al.h:106
AL_NONE = 0 	# /usr/include/AL/al.h:112
AL_FALSE = 0 	# /usr/include/AL/al.h:115
AL_TRUE = 1 	# /usr/include/AL/al.h:118
AL_SOURCE_RELATIVE = 514 	# /usr/include/AL/al.h:121
AL_CONE_INNER_ANGLE = 4097 	# /usr/include/AL/al.h:130
AL_CONE_OUTER_ANGLE = 4098 	# /usr/include/AL/al.h:137
AL_PITCH = 4099 	# /usr/include/AL/al.h:145
AL_POSITION = 4100 	# /usr/include/AL/al.h:157
AL_DIRECTION = 4101 	# /usr/include/AL/al.h:160
AL_VELOCITY = 4102 	# /usr/include/AL/al.h:163
AL_LOOPING = 4103 	# /usr/include/AL/al.h:171
AL_BUFFER = 4105 	# /usr/include/AL/al.h:178
AL_GAIN = 4106 	# /usr/include/AL/al.h:191
AL_MIN_GAIN = 4109 	# /usr/include/AL/al.h:200
AL_MAX_GAIN = 4110 	# /usr/include/AL/al.h:209
AL_ORIENTATION = 4111 	# /usr/include/AL/al.h:216
AL_SOURCE_STATE = 4112 	# /usr/include/AL/al.h:221
AL_INITIAL = 4113 	# /usr/include/AL/al.h:222
AL_PLAYING = 4114 	# /usr/include/AL/al.h:223
AL_PAUSED = 4115 	# /usr/include/AL/al.h:224
AL_STOPPED = 4116 	# /usr/include/AL/al.h:225
AL_BUFFERS_QUEUED = 4117 	# /usr/include/AL/al.h:230
AL_BUFFERS_PROCESSED = 4118 	# /usr/include/AL/al.h:231
AL_SEC_OFFSET = 4132 	# /usr/include/AL/al.h:236
AL_SAMPLE_OFFSET = 4133 	# /usr/include/AL/al.h:237
AL_BYTE_OFFSET = 4134 	# /usr/include/AL/al.h:238
AL_SOURCE_TYPE = 4135 	# /usr/include/AL/al.h:246
AL_STATIC = 4136 	# /usr/include/AL/al.h:247
AL_STREAMING = 4137 	# /usr/include/AL/al.h:248
AL_UNDETERMINED = 4144 	# /usr/include/AL/al.h:249
AL_FORMAT_MONO8 = 4352 	# /usr/include/AL/al.h:252
AL_FORMAT_MONO16 = 4353 	# /usr/include/AL/al.h:253
AL_FORMAT_STEREO8 = 4354 	# /usr/include/AL/al.h:254
AL_FORMAT_STEREO16 = 4355 	# /usr/include/AL/al.h:255
AL_REFERENCE_DISTANCE = 4128 	# /usr/include/AL/al.h:265
AL_ROLLOFF_FACTOR = 4129 	# /usr/include/AL/al.h:273
AL_CONE_OUTER_GAIN = 4130 	# /usr/include/AL/al.h:282
AL_MAX_DISTANCE = 4131 	# /usr/include/AL/al.h:292
AL_FREQUENCY = 8193 	# /usr/include/AL/al.h:300
AL_BITS = 8194 	# /usr/include/AL/al.h:301
AL_CHANNELS = 8195 	# /usr/include/AL/al.h:302
AL_SIZE = 8196 	# /usr/include/AL/al.h:303
AL_UNUSED = 8208 	# /usr/include/AL/al.h:310
AL_PENDING = 8209 	# /usr/include/AL/al.h:311
AL_PROCESSED = 8210 	# /usr/include/AL/al.h:312
AL_NO_ERROR = 0 	# /usr/include/AL/al.h:316
AL_INVALID_NAME = 40961 	# /usr/include/AL/al.h:321
AL_INVALID_ENUM = 40962 	# /usr/include/AL/al.h:326
AL_INVALID_VALUE = 40963 	# /usr/include/AL/al.h:331
AL_INVALID_OPERATION = 40964 	# /usr/include/AL/al.h:336
AL_OUT_OF_MEMORY = 40965 	# /usr/include/AL/al.h:342
AL_VENDOR = 45057 	# /usr/include/AL/al.h:346
AL_VERSION = 45058 	# /usr/include/AL/al.h:347
AL_RENDERER = 45059 	# /usr/include/AL/al.h:348
AL_EXTENSIONS = 45060 	# /usr/include/AL/al.h:349
AL_DOPPLER_FACTOR = 49152 	# /usr/include/AL/al.h:356
AL_DOPPLER_VELOCITY = 49153 	# /usr/include/AL/al.h:361
AL_SPEED_OF_SOUND = 49155 	# /usr/include/AL/al.h:366
AL_DISTANCE_MODEL = 53248 	# /usr/include/AL/al.h:375
AL_INVERSE_DISTANCE = 53249 	# /usr/include/AL/al.h:376
AL_INVERSE_DISTANCE_CLAMPED = 53250 	# /usr/include/AL/al.h:377
AL_LINEAR_DISTANCE = 53251 	# /usr/include/AL/al.h:378
AL_LINEAR_DISTANCE_CLAMPED = 53252 	# /usr/include/AL/al.h:379
AL_EXPONENT_DISTANCE = 53253 	# /usr/include/AL/al.h:380
AL_EXPONENT_DISTANCE_CLAMPED = 53254 	# /usr/include/AL/al.h:381
# /usr/include/AL/al.h:386
alEnable = _lib.alEnable
alEnable.restype = None
alEnable.argtypes = [ALenum]

# /usr/include/AL/al.h:388
alDisable = _lib.alDisable
alDisable.restype = None
alDisable.argtypes = [ALenum]

# /usr/include/AL/al.h:390
alIsEnabled = _lib.alIsEnabled
alIsEnabled.restype = ALboolean
alIsEnabled.argtypes = [ALenum]

# /usr/include/AL/al.h:396
alGetString = _lib.alGetString
alGetString.restype = POINTER(ALchar)
alGetString.argtypes = [ALenum]

# /usr/include/AL/al.h:398
alGetBooleanv = _lib.alGetBooleanv
alGetBooleanv.restype = None
alGetBooleanv.argtypes = [ALenum, POINTER(ALboolean)]

# /usr/include/AL/al.h:400
alGetIntegerv = _lib.alGetIntegerv
alGetIntegerv.restype = None
alGetIntegerv.argtypes = [ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:402
alGetFloatv = _lib.alGetFloatv
alGetFloatv.restype = None
alGetFloatv.argtypes = [ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:404
alGetDoublev = _lib.alGetDoublev
alGetDoublev.restype = None
alGetDoublev.argtypes = [ALenum, POINTER(ALdouble)]

# /usr/include/AL/al.h:406
alGetBoolean = _lib.alGetBoolean
alGetBoolean.restype = ALboolean
alGetBoolean.argtypes = [ALenum]

# /usr/include/AL/al.h:408
alGetInteger = _lib.alGetInteger
alGetInteger.restype = ALint
alGetInteger.argtypes = [ALenum]

# /usr/include/AL/al.h:410
alGetFloat = _lib.alGetFloat
alGetFloat.restype = ALfloat
alGetFloat.argtypes = [ALenum]

# /usr/include/AL/al.h:412
alGetDouble = _lib.alGetDouble
alGetDouble.restype = ALdouble
alGetDouble.argtypes = [ALenum]

# /usr/include/AL/al.h:419
alGetError = _lib.alGetError
alGetError.restype = ALenum
alGetError.argtypes = []

# /usr/include/AL/al.h:427
alIsExtensionPresent = _lib.alIsExtensionPresent
alIsExtensionPresent.restype = ALboolean
alIsExtensionPresent.argtypes = [POINTER(ALchar)]

# /usr/include/AL/al.h:429
alGetProcAddress = _lib.alGetProcAddress
alGetProcAddress.restype = POINTER(c_void)
alGetProcAddress.argtypes = [POINTER(ALchar)]

# /usr/include/AL/al.h:431
alGetEnumValue = _lib.alGetEnumValue
alGetEnumValue.restype = ALenum
alGetEnumValue.argtypes = [POINTER(ALchar)]

# /usr/include/AL/al.h:450
alListenerf = _lib.alListenerf
alListenerf.restype = None
alListenerf.argtypes = [ALenum, ALfloat]

# /usr/include/AL/al.h:452
alListener3f = _lib.alListener3f
alListener3f.restype = None
alListener3f.argtypes = [ALenum, ALfloat, ALfloat, ALfloat]

# /usr/include/AL/al.h:454
alListenerfv = _lib.alListenerfv
alListenerfv.restype = None
alListenerfv.argtypes = [ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:456
alListeneri = _lib.alListeneri
alListeneri.restype = None
alListeneri.argtypes = [ALenum, ALint]

# /usr/include/AL/al.h:458
#alListener3i = _lib.alListener3i
#alListener3i.restype = None
#alListener3i.argtypes = [ALenum, ALint, ALint, ALint]

# /usr/include/AL/al.h:460
#alListeneriv = _lib.alListeneriv
#alListeneriv.restype = None
#alListeneriv.argtypes = [ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:465
alGetListenerf = _lib.alGetListenerf
alGetListenerf.restype = None
alGetListenerf.argtypes = [ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:467
alGetListener3f = _lib.alGetListener3f
alGetListener3f.restype = None
alGetListener3f.argtypes = [ALenum, POINTER(ALfloat), POINTER(ALfloat), POINTER(ALfloat)]

# /usr/include/AL/al.h:469
alGetListenerfv = _lib.alGetListenerfv
alGetListenerfv.restype = None
alGetListenerfv.argtypes = [ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:471
alGetListeneri = _lib.alGetListeneri
alGetListeneri.restype = None
alGetListeneri.argtypes = [ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:473
alGetListener3i = _lib.alGetListener3i
alGetListener3i.restype = None
alGetListener3i.argtypes = [ALenum, POINTER(ALint), POINTER(ALint), POINTER(ALint)]

# /usr/include/AL/al.h:475
alGetListeneriv = _lib.alGetListeneriv
alGetListeneriv.restype = None
alGetListeneriv.argtypes = [ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:512
alGenSources = _lib.alGenSources
alGenSources.restype = None
alGenSources.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:515
alDeleteSources = _lib.alDeleteSources
alDeleteSources.restype = None
alDeleteSources.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:518
alIsSource = _lib.alIsSource
alIsSource.restype = ALboolean
alIsSource.argtypes = [ALuint]

# /usr/include/AL/al.h:523
alSourcef = _lib.alSourcef
alSourcef.restype = None
alSourcef.argtypes = [ALuint, ALenum, ALfloat]

# /usr/include/AL/al.h:525
alSource3f = _lib.alSource3f
alSource3f.restype = None
alSource3f.argtypes = [ALuint, ALenum, ALfloat, ALfloat, ALfloat]

# /usr/include/AL/al.h:527
alSourcefv = _lib.alSourcefv
alSourcefv.restype = None
alSourcefv.argtypes = [ALuint, ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:529
alSourcei = _lib.alSourcei
alSourcei.restype = None
alSourcei.argtypes = [ALuint, ALenum, ALint]

# /usr/include/AL/al.h:531
#alSource3i = _lib.alSource3i
#alSource3i.restype = None
#alSource3i.argtypes = [ALuint, ALenum, ALint, ALint, ALint]

# /usr/include/AL/al.h:533
#alSourceiv = _lib.alSourceiv
#alSourceiv.restype = None
#alSourceiv.argtypes = [ALuint, ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:538
alGetSourcef = _lib.alGetSourcef
alGetSourcef.restype = None
alGetSourcef.argtypes = [ALuint, ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:540
alGetSource3f = _lib.alGetSource3f
alGetSource3f.restype = None
alGetSource3f.argtypes = [ALuint, ALenum, POINTER(ALfloat), POINTER(ALfloat), POINTER(ALfloat)]

# /usr/include/AL/al.h:542
alGetSourcefv = _lib.alGetSourcefv
alGetSourcefv.restype = None
alGetSourcefv.argtypes = [ALuint, ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:544
alGetSourcei = _lib.alGetSourcei
alGetSourcei.restype = None
alGetSourcei.argtypes = [ALuint, ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:546
#alGetSource3i = _lib.alGetSource3i
#alGetSource3i.restype = None
#alGetSource3i.argtypes = [ALuint, ALenum, POINTER(ALint), POINTER(ALint), POINTER(ALint)]

# /usr/include/AL/al.h:548
alGetSourceiv = _lib.alGetSourceiv
alGetSourceiv.restype = None
alGetSourceiv.argtypes = [ALuint, ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:556
alSourcePlayv = _lib.alSourcePlayv
alSourcePlayv.restype = None
alSourcePlayv.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:559
alSourceStopv = _lib.alSourceStopv
alSourceStopv.restype = None
alSourceStopv.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:562
alSourceRewindv = _lib.alSourceRewindv
alSourceRewindv.restype = None
alSourceRewindv.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:565
alSourcePausev = _lib.alSourcePausev
alSourcePausev.restype = None
alSourcePausev.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:572
alSourcePlay = _lib.alSourcePlay
alSourcePlay.restype = None
alSourcePlay.argtypes = [ALuint]

# /usr/include/AL/al.h:575
alSourceStop = _lib.alSourceStop
alSourceStop.restype = None
alSourceStop.argtypes = [ALuint]

# /usr/include/AL/al.h:578
alSourceRewind = _lib.alSourceRewind
alSourceRewind.restype = None
alSourceRewind.argtypes = [ALuint]

# /usr/include/AL/al.h:581
alSourcePause = _lib.alSourcePause
alSourcePause.restype = None
alSourcePause.argtypes = [ALuint]

# /usr/include/AL/al.h:586
alSourceQueueBuffers = _lib.alSourceQueueBuffers
alSourceQueueBuffers.restype = None
alSourceQueueBuffers.argtypes = [ALuint, ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:588
alSourceUnqueueBuffers = _lib.alSourceUnqueueBuffers
alSourceUnqueueBuffers.restype = None
alSourceUnqueueBuffers.argtypes = [ALuint, ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:606
alGenBuffers = _lib.alGenBuffers
alGenBuffers.restype = None
alGenBuffers.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:609
alDeleteBuffers = _lib.alDeleteBuffers
alDeleteBuffers.restype = None
alDeleteBuffers.argtypes = [ALsizei, POINTER(ALuint)]

# /usr/include/AL/al.h:612
alIsBuffer = _lib.alIsBuffer
alIsBuffer.restype = ALboolean
alIsBuffer.argtypes = [ALuint]

# /usr/include/AL/al.h:615
alBufferData = _lib.alBufferData
alBufferData.restype = None
alBufferData.argtypes = [ALuint, ALenum, POINTER(ALvoid), ALsizei, ALsizei]

# /usr/include/AL/al.h:620
alBufferf = _lib.alBufferf
alBufferf.restype = None
alBufferf.argtypes = [ALuint, ALenum, ALfloat]

# /usr/include/AL/al.h:622
alBuffer3f = _lib.alBuffer3f
alBuffer3f.restype = None
alBuffer3f.argtypes = [ALuint, ALenum, ALfloat, ALfloat, ALfloat]

# /usr/include/AL/al.h:624
alBufferfv = _lib.alBufferfv
alBufferfv.restype = None
alBufferfv.argtypes = [ALuint, ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:626
alBufferi = _lib.alBufferi
alBufferi.restype = None
alBufferi.argtypes = [ALuint, ALenum, ALint]

# /usr/include/AL/al.h:628
alBuffer3i = _lib.alBuffer3i
alBuffer3i.restype = None
alBuffer3i.argtypes = [ALuint, ALenum, ALint, ALint, ALint]

# /usr/include/AL/al.h:630
alBufferiv = _lib.alBufferiv
alBufferiv.restype = None
alBufferiv.argtypes = [ALuint, ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:635
alGetBufferf = _lib.alGetBufferf
alGetBufferf.restype = None
alGetBufferf.argtypes = [ALuint, ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:637
alGetBuffer3f = _lib.alGetBuffer3f
alGetBuffer3f.restype = None
alGetBuffer3f.argtypes = [ALuint, ALenum, POINTER(ALfloat), POINTER(ALfloat), POINTER(ALfloat)]

# /usr/include/AL/al.h:639
alGetBufferfv = _lib.alGetBufferfv
alGetBufferfv.restype = None
alGetBufferfv.argtypes = [ALuint, ALenum, POINTER(ALfloat)]

# /usr/include/AL/al.h:641
alGetBufferi = _lib.alGetBufferi
alGetBufferi.restype = None
alGetBufferi.argtypes = [ALuint, ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:643
alGetBuffer3i = _lib.alGetBuffer3i
alGetBuffer3i.restype = None
alGetBuffer3i.argtypes = [ALuint, ALenum, POINTER(ALint), POINTER(ALint), POINTER(ALint)]

# /usr/include/AL/al.h:645
alGetBufferiv = _lib.alGetBufferiv
alGetBufferiv.restype = None
alGetBufferiv.argtypes = [ALuint, ALenum, POINTER(ALint)]

# /usr/include/AL/al.h:651
alDopplerFactor = _lib.alDopplerFactor
alDopplerFactor.restype = None
alDopplerFactor.argtypes = [ALfloat]

# /usr/include/AL/al.h:653
alDopplerVelocity = _lib.alDopplerVelocity
alDopplerVelocity.restype = None
alDopplerVelocity.argtypes = [ALfloat]

# /usr/include/AL/al.h:655
alSpeedOfSound = _lib.alSpeedOfSound
alSpeedOfSound.restype = None
alSpeedOfSound.argtypes = [ALfloat]

# /usr/include/AL/al.h:657
alDistanceModel = _lib.alDistanceModel
alDistanceModel.restype = None
alDistanceModel.argtypes = [ALenum]

LPALENABLE = CFUNCTYPE(None, ALenum) 	# /usr/include/AL/al.h:662
LPALDISABLE = CFUNCTYPE(None, ALenum) 	# /usr/include/AL/al.h:663
LPALISENABLED = CFUNCTYPE(ALboolean, ALenum) 	# /usr/include/AL/al.h:664
LPALGETSTRING = CFUNCTYPE(POINTER(ALchar), ALenum) 	# /usr/include/AL/al.h:665
LPALGETBOOLEANV = CFUNCTYPE(None, ALenum, POINTER(ALboolean)) 	# /usr/include/AL/al.h:666
LPALGETINTEGERV = CFUNCTYPE(None, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:667
LPALGETFLOATV = CFUNCTYPE(None, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:668
LPALGETDOUBLEV = CFUNCTYPE(None, ALenum, POINTER(ALdouble)) 	# /usr/include/AL/al.h:669
LPALGETBOOLEAN = CFUNCTYPE(ALboolean, ALenum) 	# /usr/include/AL/al.h:670
LPALGETINTEGER = CFUNCTYPE(ALint, ALenum) 	# /usr/include/AL/al.h:671
LPALGETFLOAT = CFUNCTYPE(ALfloat, ALenum) 	# /usr/include/AL/al.h:672
LPALGETDOUBLE = CFUNCTYPE(ALdouble, ALenum) 	# /usr/include/AL/al.h:673
LPALGETERROR = CFUNCTYPE(ALenum) 	# /usr/include/AL/al.h:674
LPALISEXTENSIONPRESENT = CFUNCTYPE(ALboolean, POINTER(ALchar)) 	# /usr/include/AL/al.h:675
LPALGETPROCADDRESS = CFUNCTYPE(POINTER(c_void), POINTER(ALchar)) 	# /usr/include/AL/al.h:676
LPALGETENUMVALUE = CFUNCTYPE(ALenum, POINTER(ALchar)) 	# /usr/include/AL/al.h:677
LPALLISTENERF = CFUNCTYPE(None, ALenum, ALfloat) 	# /usr/include/AL/al.h:678
LPALLISTENER3F = CFUNCTYPE(None, ALenum, ALfloat, ALfloat, ALfloat) 	# /usr/include/AL/al.h:679
LPALLISTENERFV = CFUNCTYPE(None, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:680
LPALLISTENERI = CFUNCTYPE(None, ALenum, ALint) 	# /usr/include/AL/al.h:681
LPALLISTENER3I = CFUNCTYPE(None, ALenum, ALint, ALint, ALint) 	# /usr/include/AL/al.h:682
LPALLISTENERIV = CFUNCTYPE(None, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:683
LPALGETLISTENERF = CFUNCTYPE(None, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:684
LPALGETLISTENER3F = CFUNCTYPE(None, ALenum, POINTER(ALfloat), POINTER(ALfloat), POINTER(ALfloat)) 	# /usr/include/AL/al.h:685
LPALGETLISTENERFV = CFUNCTYPE(None, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:686
LPALGETLISTENERI = CFUNCTYPE(None, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:687
LPALGETLISTENER3I = CFUNCTYPE(None, ALenum, POINTER(ALint), POINTER(ALint), POINTER(ALint)) 	# /usr/include/AL/al.h:688
LPALGETLISTENERIV = CFUNCTYPE(None, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:689
LPALGENSOURCES = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:690
LPALDELETESOURCES = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:691
LPALISSOURCE = CFUNCTYPE(ALboolean, ALuint) 	# /usr/include/AL/al.h:692
LPALSOURCEF = CFUNCTYPE(None, ALuint, ALenum, ALfloat) 	# /usr/include/AL/al.h:693
LPALSOURCE3F = CFUNCTYPE(None, ALuint, ALenum, ALfloat, ALfloat, ALfloat) 	# /usr/include/AL/al.h:694
LPALSOURCEFV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:695
LPALSOURCEI = CFUNCTYPE(None, ALuint, ALenum, ALint) 	# /usr/include/AL/al.h:696
LPALSOURCE3I = CFUNCTYPE(None, ALuint, ALenum, ALint, ALint, ALint) 	# /usr/include/AL/al.h:697
LPALSOURCEIV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:698
LPALGETSOURCEF = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:699
LPALGETSOURCE3F = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat), POINTER(ALfloat), POINTER(ALfloat)) 	# /usr/include/AL/al.h:700
LPALGETSOURCEFV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:701
LPALGETSOURCEI = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:702
LPALGETSOURCE3I = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint), POINTER(ALint), POINTER(ALint)) 	# /usr/include/AL/al.h:703
LPALGETSOURCEIV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:704
LPALSOURCEPLAYV = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:705
LPALSOURCESTOPV = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:706
LPALSOURCEREWINDV = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:707
LPALSOURCEPAUSEV = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:708
LPALSOURCEPLAY = CFUNCTYPE(None, ALuint) 	# /usr/include/AL/al.h:709
LPALSOURCESTOP = CFUNCTYPE(None, ALuint) 	# /usr/include/AL/al.h:710
LPALSOURCEREWIND = CFUNCTYPE(None, ALuint) 	# /usr/include/AL/al.h:711
LPALSOURCEPAUSE = CFUNCTYPE(None, ALuint) 	# /usr/include/AL/al.h:712
LPALSOURCEQUEUEBUFFERS = CFUNCTYPE(None, ALuint, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:713
LPALSOURCEUNQUEUEBUFFERS = CFUNCTYPE(None, ALuint, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:714
LPALGENBUFFERS = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:715
LPALDELETEBUFFERS = CFUNCTYPE(None, ALsizei, POINTER(ALuint)) 	# /usr/include/AL/al.h:716
LPALISBUFFER = CFUNCTYPE(ALboolean, ALuint) 	# /usr/include/AL/al.h:717
LPALBUFFERDATA = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALvoid), ALsizei, ALsizei) 	# /usr/include/AL/al.h:718
LPALBUFFERF = CFUNCTYPE(None, ALuint, ALenum, ALfloat) 	# /usr/include/AL/al.h:719
LPALBUFFER3F = CFUNCTYPE(None, ALuint, ALenum, ALfloat, ALfloat, ALfloat) 	# /usr/include/AL/al.h:720
LPALBUFFERFV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:721
LPALBUFFERI = CFUNCTYPE(None, ALuint, ALenum, ALint) 	# /usr/include/AL/al.h:722
LPALBUFFER3I = CFUNCTYPE(None, ALuint, ALenum, ALint, ALint, ALint) 	# /usr/include/AL/al.h:723
LPALBUFFERIV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:724
LPALGETBUFFERF = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:725
LPALGETBUFFER3F = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat), POINTER(ALfloat), POINTER(ALfloat)) 	# /usr/include/AL/al.h:726
LPALGETBUFFERFV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALfloat)) 	# /usr/include/AL/al.h:727
LPALGETBUFFERI = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:728
LPALGETBUFFER3I = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint), POINTER(ALint), POINTER(ALint)) 	# /usr/include/AL/al.h:729
LPALGETBUFFERIV = CFUNCTYPE(None, ALuint, ALenum, POINTER(ALint)) 	# /usr/include/AL/al.h:730
LPALDOPPLERFACTOR = CFUNCTYPE(None, ALfloat) 	# /usr/include/AL/al.h:731
LPALDOPPLERVELOCITY = CFUNCTYPE(None, ALfloat) 	# /usr/include/AL/al.h:732
LPALSPEEDOFSOUND = CFUNCTYPE(None, ALfloat) 	# /usr/include/AL/al.h:733
LPALDISTANCEMODEL = CFUNCTYPE(None, ALenum) 	# /usr/include/AL/al.h:734

__all__ = ['AL_API', 'ALAPI', 'AL_INVALID', 'AL_ILLEGAL_ENUM',
'AL_ILLEGAL_COMMAND', 'ALboolean', 'ALchar', 'ALbyte', 'ALubyte', 'ALshort',
'ALushort', 'ALint', 'ALuint', 'ALsizei', 'ALenum', 'ALfloat', 'ALdouble',
'ALvoid', 'AL_NONE', 'AL_FALSE', 'AL_TRUE', 'AL_SOURCE_RELATIVE',
'AL_CONE_INNER_ANGLE', 'AL_CONE_OUTER_ANGLE', 'AL_PITCH', 'AL_POSITION',
'AL_DIRECTION', 'AL_VELOCITY', 'AL_LOOPING', 'AL_BUFFER', 'AL_GAIN',
'AL_MIN_GAIN', 'AL_MAX_GAIN', 'AL_ORIENTATION', 'AL_SOURCE_STATE',
'AL_INITIAL', 'AL_PLAYING', 'AL_PAUSED', 'AL_STOPPED', 'AL_BUFFERS_QUEUED',
'AL_BUFFERS_PROCESSED', 'AL_SEC_OFFSET', 'AL_SAMPLE_OFFSET', 'AL_BYTE_OFFSET',
'AL_SOURCE_TYPE', 'AL_STATIC', 'AL_STREAMING', 'AL_UNDETERMINED',
'AL_FORMAT_MONO8', 'AL_FORMAT_MONO16', 'AL_FORMAT_STEREO8',
'AL_FORMAT_STEREO16', 'AL_REFERENCE_DISTANCE', 'AL_ROLLOFF_FACTOR',
'AL_CONE_OUTER_GAIN', 'AL_MAX_DISTANCE', 'AL_FREQUENCY', 'AL_BITS',
'AL_CHANNELS', 'AL_SIZE', 'AL_UNUSED', 'AL_PENDING', 'AL_PROCESSED',
'AL_NO_ERROR', 'AL_INVALID_NAME', 'AL_INVALID_ENUM', 'AL_INVALID_VALUE',
'AL_INVALID_OPERATION', 'AL_OUT_OF_MEMORY', 'AL_VENDOR', 'AL_VERSION',
'AL_RENDERER', 'AL_EXTENSIONS', 'AL_DOPPLER_FACTOR', 'AL_DOPPLER_VELOCITY',
'AL_SPEED_OF_SOUND', 'AL_DISTANCE_MODEL', 'AL_INVERSE_DISTANCE',
'AL_INVERSE_DISTANCE_CLAMPED', 'AL_LINEAR_DISTANCE',
'AL_LINEAR_DISTANCE_CLAMPED', 'AL_EXPONENT_DISTANCE',
'AL_EXPONENT_DISTANCE_CLAMPED', 'alEnable', 'alDisable', 'alIsEnabled',
'alGetString', 'alGetBooleanv', 'alGetIntegerv', 'alGetFloatv',
'alGetDoublev', 'alGetBoolean', 'alGetInteger', 'alGetFloat', 'alGetDouble',
'alGetError', 'alIsExtensionPresent', 'alGetProcAddress', 'alGetEnumValue',
'alListenerf', 'alListener3f', 'alListenerfv', 'alListeneri', 'alListener3i',
'alListeneriv', 'alGetListenerf', 'alGetListener3f', 'alGetListenerfv',
'alGetListeneri', 'alGetListener3i', 'alGetListeneriv', 'alGenSources',
'alDeleteSources', 'alIsSource', 'alSourcef', 'alSource3f', 'alSourcefv',
'alSourcei', 'alSource3i', 'alSourceiv', 'alGetSourcef', 'alGetSource3f',
'alGetSourcefv', 'alGetSourcei', 'alGetSource3i', 'alGetSourceiv',
'alSourcePlayv', 'alSourceStopv', 'alSourceRewindv', 'alSourcePausev',
'alSourcePlay', 'alSourceStop', 'alSourceRewind', 'alSourcePause',
'alSourceQueueBuffers', 'alSourceUnqueueBuffers', 'alGenBuffers',
'alDeleteBuffers', 'alIsBuffer', 'alBufferData', 'alBufferf', 'alBuffer3f',
'alBufferfv', 'alBufferi', 'alBuffer3i', 'alBufferiv', 'alGetBufferf',
'alGetBuffer3f', 'alGetBufferfv', 'alGetBufferi', 'alGetBuffer3i',
'alGetBufferiv', 'alDopplerFactor', 'alDopplerVelocity', 'alSpeedOfSound',
'alDistanceModel', 'LPALENABLE', 'LPALDISABLE', 'LPALISENABLED',
'LPALGETSTRING', 'LPALGETBOOLEANV', 'LPALGETINTEGERV', 'LPALGETFLOATV',
'LPALGETDOUBLEV', 'LPALGETBOOLEAN', 'LPALGETINTEGER', 'LPALGETFLOAT',
'LPALGETDOUBLE', 'LPALGETERROR', 'LPALISEXTENSIONPRESENT',
'LPALGETPROCADDRESS', 'LPALGETENUMVALUE', 'LPALLISTENERF', 'LPALLISTENER3F',
'LPALLISTENERFV', 'LPALLISTENERI', 'LPALLISTENER3I', 'LPALLISTENERIV',
'LPALGETLISTENERF', 'LPALGETLISTENER3F', 'LPALGETLISTENERFV',
'LPALGETLISTENERI', 'LPALGETLISTENER3I', 'LPALGETLISTENERIV',
'LPALGENSOURCES', 'LPALDELETESOURCES', 'LPALISSOURCE', 'LPALSOURCEF',
'LPALSOURCE3F', 'LPALSOURCEFV', 'LPALSOURCEI', 'LPALSOURCE3I', 'LPALSOURCEIV',
'LPALGETSOURCEF', 'LPALGETSOURCE3F', 'LPALGETSOURCEFV', 'LPALGETSOURCEI',
'LPALGETSOURCE3I', 'LPALGETSOURCEIV', 'LPALSOURCEPLAYV', 'LPALSOURCESTOPV',
'LPALSOURCEREWINDV', 'LPALSOURCEPAUSEV', 'LPALSOURCEPLAY', 'LPALSOURCESTOP',
'LPALSOURCEREWIND', 'LPALSOURCEPAUSE', 'LPALSOURCEQUEUEBUFFERS',
'LPALSOURCEUNQUEUEBUFFERS', 'LPALGENBUFFERS', 'LPALDELETEBUFFERS',
'LPALISBUFFER', 'LPALBUFFERDATA', 'LPALBUFFERF', 'LPALBUFFER3F',
'LPALBUFFERFV', 'LPALBUFFERI', 'LPALBUFFER3I', 'LPALBUFFERIV',
'LPALGETBUFFERF', 'LPALGETBUFFER3F', 'LPALGETBUFFERFV', 'LPALGETBUFFERI',
'LPALGETBUFFER3I', 'LPALGETBUFFERIV', 'LPALDOPPLERFACTOR',
'LPALDOPPLERVELOCITY', 'LPALSPEEDOFSOUND', 'LPALDISTANCEMODEL']
