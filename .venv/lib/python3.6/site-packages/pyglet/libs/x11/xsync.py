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
'''Wrapper for Xext

Generated with:
tools/genwrappers.py xsync

Do not modify this file.
'''
from __future__ import absolute_import

__docformat__ =  'restructuredtext'
__version__ = '$Id$'

import ctypes
from ctypes import *

import pyglet.lib

_lib = pyglet.lib.load_library('Xext')

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


# XXX DODGY relative import of xlib.py, which contains XID etc definitions.
# can't use wrapped import which gave
#   import pyglet.window.xlib.xlib
# because Python has the lamest import semantics and can't handle that kind of
# recursive import, even though it's the same as
from . import xlib

SYNC_MAJOR_VERSION = 3 	# /usr/include/X11/extensions/sync.h:4901
SYNC_MINOR_VERSION = 0 	# /usr/include/X11/extensions/sync.h:4902
X_SyncInitialize = 0 	# /usr/include/X11/extensions/sync.h:4904
X_SyncListSystemCounters = 1 	# /usr/include/X11/extensions/sync.h:4905
X_SyncCreateCounter = 2 	# /usr/include/X11/extensions/sync.h:4906
X_SyncSetCounter = 3 	# /usr/include/X11/extensions/sync.h:4907
X_SyncChangeCounter = 4 	# /usr/include/X11/extensions/sync.h:4908
X_SyncQueryCounter = 5 	# /usr/include/X11/extensions/sync.h:4909
X_SyncDestroyCounter = 6 	# /usr/include/X11/extensions/sync.h:4910
X_SyncAwait = 7 	# /usr/include/X11/extensions/sync.h:4911
X_SyncCreateAlarm = 8 	# /usr/include/X11/extensions/sync.h:4912
X_SyncChangeAlarm = 9 	# /usr/include/X11/extensions/sync.h:4913
X_SyncQueryAlarm = 10 	# /usr/include/X11/extensions/sync.h:4914
X_SyncDestroyAlarm = 11 	# /usr/include/X11/extensions/sync.h:4915
X_SyncSetPriority = 12 	# /usr/include/X11/extensions/sync.h:4916
X_SyncGetPriority = 13 	# /usr/include/X11/extensions/sync.h:4917
XSyncCounterNotify = 0 	# /usr/include/X11/extensions/sync.h:4919
XSyncAlarmNotify = 1 	# /usr/include/X11/extensions/sync.h:4920
XSyncAlarmNotifyMask = 2 	# /usr/include/X11/extensions/sync.h:4921
XSyncNumberEvents = 2 	# /usr/include/X11/extensions/sync.h:4923
XSyncBadCounter = 0 	# /usr/include/X11/extensions/sync.h:4925
XSyncBadAlarm = 1 	# /usr/include/X11/extensions/sync.h:4926
XSyncNumberErrors = 2 	# /usr/include/X11/extensions/sync.h:4927
XSyncCACounter = 1 	# /usr/include/X11/extensions/sync.h:4932
XSyncCAValueType = 2 	# /usr/include/X11/extensions/sync.h:4933
XSyncCAValue = 4 	# /usr/include/X11/extensions/sync.h:4934
XSyncCATestType = 8 	# /usr/include/X11/extensions/sync.h:4935
XSyncCADelta = 16 	# /usr/include/X11/extensions/sync.h:4936
XSyncCAEvents = 32 	# /usr/include/X11/extensions/sync.h:4937
enum_anon_93 = c_int
XSyncAbsolute = 0
XSyncRelative = 1
XSyncValueType = enum_anon_93 	# /usr/include/X11/extensions/sync.h:4945
enum_anon_94 = c_int
XSyncPositiveTransition = 0
XSyncNegativeTransition = 1
XSyncPositiveComparison = 2
XSyncNegativeComparison = 3
XSyncTestType = enum_anon_94 	# /usr/include/X11/extensions/sync.h:4955
enum_anon_95 = c_int
XSyncAlarmActive = 0
XSyncAlarmInactive = 1
XSyncAlarmDestroyed = 2
XSyncAlarmState = enum_anon_95 	# /usr/include/X11/extensions/sync.h:4964
XID = xlib.XID
XSyncCounter = XID 	# /usr/include/X11/extensions/sync.h:4967
XSyncAlarm = XID 	# /usr/include/X11/extensions/sync.h:4968
class struct__XSyncValue(Structure):
    __slots__ = [
        'hi',
        'lo',
    ]
struct__XSyncValue._fields_ = [
    ('hi', c_int),
    ('lo', c_uint),
]

XSyncValue = struct__XSyncValue 	# /usr/include/X11/extensions/sync.h:4972
# /usr/include/X11/extensions/sync.h:4980
XSyncIntToValue = _lib.XSyncIntToValue
XSyncIntToValue.restype = None
XSyncIntToValue.argtypes = [POINTER(XSyncValue), c_int]

# /usr/include/X11/extensions/sync.h:4985
XSyncIntsToValue = _lib.XSyncIntsToValue
XSyncIntsToValue.restype = None
XSyncIntsToValue.argtypes = [POINTER(XSyncValue), c_uint, c_int]

Bool = xlib.Bool
# /usr/include/X11/extensions/sync.h:4991
XSyncValueGreaterThan = _lib.XSyncValueGreaterThan
XSyncValueGreaterThan.restype = Bool
XSyncValueGreaterThan.argtypes = [XSyncValue, XSyncValue]

# /usr/include/X11/extensions/sync.h:4996
XSyncValueLessThan = _lib.XSyncValueLessThan
XSyncValueLessThan.restype = Bool
XSyncValueLessThan.argtypes = [XSyncValue, XSyncValue]

# /usr/include/X11/extensions/sync.h:5001
XSyncValueGreaterOrEqual = _lib.XSyncValueGreaterOrEqual
XSyncValueGreaterOrEqual.restype = Bool
XSyncValueGreaterOrEqual.argtypes = [XSyncValue, XSyncValue]

# /usr/include/X11/extensions/sync.h:5006
XSyncValueLessOrEqual = _lib.XSyncValueLessOrEqual
XSyncValueLessOrEqual.restype = Bool
XSyncValueLessOrEqual.argtypes = [XSyncValue, XSyncValue]

# /usr/include/X11/extensions/sync.h:5011
XSyncValueEqual = _lib.XSyncValueEqual
XSyncValueEqual.restype = Bool
XSyncValueEqual.argtypes = [XSyncValue, XSyncValue]

# /usr/include/X11/extensions/sync.h:5016
XSyncValueIsNegative = _lib.XSyncValueIsNegative
XSyncValueIsNegative.restype = Bool
XSyncValueIsNegative.argtypes = [XSyncValue]

# /usr/include/X11/extensions/sync.h:5020
XSyncValueIsZero = _lib.XSyncValueIsZero
XSyncValueIsZero.restype = Bool
XSyncValueIsZero.argtypes = [XSyncValue]

# /usr/include/X11/extensions/sync.h:5024
XSyncValueIsPositive = _lib.XSyncValueIsPositive
XSyncValueIsPositive.restype = Bool
XSyncValueIsPositive.argtypes = [XSyncValue]

# /usr/include/X11/extensions/sync.h:5028
XSyncValueLow32 = _lib.XSyncValueLow32
XSyncValueLow32.restype = c_uint
XSyncValueLow32.argtypes = [XSyncValue]

# /usr/include/X11/extensions/sync.h:5032
XSyncValueHigh32 = _lib.XSyncValueHigh32
XSyncValueHigh32.restype = c_int
XSyncValueHigh32.argtypes = [XSyncValue]

# /usr/include/X11/extensions/sync.h:5036
XSyncValueAdd = _lib.XSyncValueAdd
XSyncValueAdd.restype = None
XSyncValueAdd.argtypes = [POINTER(XSyncValue), XSyncValue, XSyncValue, POINTER(c_int)]

# /usr/include/X11/extensions/sync.h:5043
XSyncValueSubtract = _lib.XSyncValueSubtract
XSyncValueSubtract.restype = None
XSyncValueSubtract.argtypes = [POINTER(XSyncValue), XSyncValue, XSyncValue, POINTER(c_int)]

# /usr/include/X11/extensions/sync.h:5050
XSyncMaxValue = _lib.XSyncMaxValue
XSyncMaxValue.restype = None
XSyncMaxValue.argtypes = [POINTER(XSyncValue)]

# /usr/include/X11/extensions/sync.h:5054
XSyncMinValue = _lib.XSyncMinValue
XSyncMinValue.restype = None
XSyncMinValue.argtypes = [POINTER(XSyncValue)]

class struct__XSyncSystemCounter(Structure):
    __slots__ = [
        'name',
        'counter',
        'resolution',
    ]
struct__XSyncSystemCounter._fields_ = [
    ('name', c_char_p),
    ('counter', XSyncCounter),
    ('resolution', XSyncValue),
]

XSyncSystemCounter = struct__XSyncSystemCounter 	# /usr/include/X11/extensions/sync.h:5131
class struct_anon_96(Structure):
    __slots__ = [
        'counter',
        'value_type',
        'wait_value',
        'test_type',
    ]
struct_anon_96._fields_ = [
    ('counter', XSyncCounter),
    ('value_type', XSyncValueType),
    ('wait_value', XSyncValue),
    ('test_type', XSyncTestType),
]

XSyncTrigger = struct_anon_96 	# /usr/include/X11/extensions/sync.h:5139
class struct_anon_97(Structure):
    __slots__ = [
        'trigger',
        'event_threshold',
    ]
struct_anon_97._fields_ = [
    ('trigger', XSyncTrigger),
    ('event_threshold', XSyncValue),
]

XSyncWaitCondition = struct_anon_97 	# /usr/include/X11/extensions/sync.h:5144
class struct_anon_98(Structure):
    __slots__ = [
        'trigger',
        'delta',
        'events',
        'state',
    ]
struct_anon_98._fields_ = [
    ('trigger', XSyncTrigger),
    ('delta', XSyncValue),
    ('events', Bool),
    ('state', XSyncAlarmState),
]

XSyncAlarmAttributes = struct_anon_98 	# /usr/include/X11/extensions/sync.h:5152
class struct_anon_99(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'counter',
        'wait_value',
        'counter_value',
        'time',
        'count',
        'destroyed',
    ]
Display = xlib.Display
Time = xlib.Time
struct_anon_99._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', Bool),
    ('display', POINTER(Display)),
    ('counter', XSyncCounter),
    ('wait_value', XSyncValue),
    ('counter_value', XSyncValue),
    ('time', Time),
    ('count', c_int),
    ('destroyed', Bool),
]

XSyncCounterNotifyEvent = struct_anon_99 	# /usr/include/X11/extensions/sync.h:5169
class struct_anon_100(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'alarm',
        'counter_value',
        'alarm_value',
        'time',
        'state',
    ]
struct_anon_100._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', Bool),
    ('display', POINTER(Display)),
    ('alarm', XSyncAlarm),
    ('counter_value', XSyncValue),
    ('alarm_value', XSyncValue),
    ('time', Time),
    ('state', XSyncAlarmState),
]

XSyncAlarmNotifyEvent = struct_anon_100 	# /usr/include/X11/extensions/sync.h:5181
class struct_anon_101(Structure):
    __slots__ = [
        'type',
        'display',
        'alarm',
        'serial',
        'error_code',
        'request_code',
        'minor_code',
    ]
struct_anon_101._fields_ = [
    ('type', c_int),
    ('display', POINTER(Display)),
    ('alarm', XSyncAlarm),
    ('serial', c_ulong),
    ('error_code', c_ubyte),
    ('request_code', c_ubyte),
    ('minor_code', c_ubyte),
]

XSyncAlarmError = struct_anon_101 	# /usr/include/X11/extensions/sync.h:5195
class struct_anon_102(Structure):
    __slots__ = [
        'type',
        'display',
        'counter',
        'serial',
        'error_code',
        'request_code',
        'minor_code',
    ]
struct_anon_102._fields_ = [
    ('type', c_int),
    ('display', POINTER(Display)),
    ('counter', XSyncCounter),
    ('serial', c_ulong),
    ('error_code', c_ubyte),
    ('request_code', c_ubyte),
    ('minor_code', c_ubyte),
]

XSyncCounterError = struct_anon_102 	# /usr/include/X11/extensions/sync.h:5205
# /usr/include/X11/extensions/sync.h:5213
XSyncQueryExtension = _lib.XSyncQueryExtension
XSyncQueryExtension.restype = c_int
XSyncQueryExtension.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/sync.h:5219
XSyncInitialize = _lib.XSyncInitialize
XSyncInitialize.restype = c_int
XSyncInitialize.argtypes = [POINTER(Display), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/sync.h:5225
XSyncListSystemCounters = _lib.XSyncListSystemCounters
XSyncListSystemCounters.restype = POINTER(XSyncSystemCounter)
XSyncListSystemCounters.argtypes = [POINTER(Display), POINTER(c_int)]

# /usr/include/X11/extensions/sync.h:5230
XSyncFreeSystemCounterList = _lib.XSyncFreeSystemCounterList
XSyncFreeSystemCounterList.restype = None
XSyncFreeSystemCounterList.argtypes = [POINTER(XSyncSystemCounter)]

# /usr/include/X11/extensions/sync.h:5234
XSyncCreateCounter = _lib.XSyncCreateCounter
XSyncCreateCounter.restype = XSyncCounter
XSyncCreateCounter.argtypes = [POINTER(Display), XSyncValue]

# /usr/include/X11/extensions/sync.h:5239
XSyncSetCounter = _lib.XSyncSetCounter
XSyncSetCounter.restype = c_int
XSyncSetCounter.argtypes = [POINTER(Display), XSyncCounter, XSyncValue]

# /usr/include/X11/extensions/sync.h:5245
XSyncChangeCounter = _lib.XSyncChangeCounter
XSyncChangeCounter.restype = c_int
XSyncChangeCounter.argtypes = [POINTER(Display), XSyncCounter, XSyncValue]

# /usr/include/X11/extensions/sync.h:5251
XSyncDestroyCounter = _lib.XSyncDestroyCounter
XSyncDestroyCounter.restype = c_int
XSyncDestroyCounter.argtypes = [POINTER(Display), XSyncCounter]

# /usr/include/X11/extensions/sync.h:5256
XSyncQueryCounter = _lib.XSyncQueryCounter
XSyncQueryCounter.restype = c_int
XSyncQueryCounter.argtypes = [POINTER(Display), XSyncCounter, POINTER(XSyncValue)]

# /usr/include/X11/extensions/sync.h:5262
XSyncAwait = _lib.XSyncAwait
XSyncAwait.restype = c_int
XSyncAwait.argtypes = [POINTER(Display), POINTER(XSyncWaitCondition), c_int]

# /usr/include/X11/extensions/sync.h:5268
XSyncCreateAlarm = _lib.XSyncCreateAlarm
XSyncCreateAlarm.restype = XSyncAlarm
XSyncCreateAlarm.argtypes = [POINTER(Display), c_ulong, POINTER(XSyncAlarmAttributes)]

# /usr/include/X11/extensions/sync.h:5274
XSyncDestroyAlarm = _lib.XSyncDestroyAlarm
XSyncDestroyAlarm.restype = c_int
XSyncDestroyAlarm.argtypes = [POINTER(Display), XSyncAlarm]

# /usr/include/X11/extensions/sync.h:5279
XSyncQueryAlarm = _lib.XSyncQueryAlarm
XSyncQueryAlarm.restype = c_int
XSyncQueryAlarm.argtypes = [POINTER(Display), XSyncAlarm, POINTER(XSyncAlarmAttributes)]

# /usr/include/X11/extensions/sync.h:5285
XSyncChangeAlarm = _lib.XSyncChangeAlarm
XSyncChangeAlarm.restype = c_int
XSyncChangeAlarm.argtypes = [POINTER(Display), XSyncAlarm, c_ulong, POINTER(XSyncAlarmAttributes)]

# /usr/include/X11/extensions/sync.h:5292
XSyncSetPriority = _lib.XSyncSetPriority
XSyncSetPriority.restype = c_int
XSyncSetPriority.argtypes = [POINTER(Display), XID, c_int]

# /usr/include/X11/extensions/sync.h:5298
XSyncGetPriority = _lib.XSyncGetPriority
XSyncGetPriority.restype = c_int
XSyncGetPriority.argtypes = [POINTER(Display), XID, POINTER(c_int)]


__all__ = ['SYNC_MAJOR_VERSION', 'SYNC_MINOR_VERSION', 'X_SyncInitialize',
'X_SyncListSystemCounters', 'X_SyncCreateCounter', 'X_SyncSetCounter',
'X_SyncChangeCounter', 'X_SyncQueryCounter', 'X_SyncDestroyCounter',
'X_SyncAwait', 'X_SyncCreateAlarm', 'X_SyncChangeAlarm', 'X_SyncQueryAlarm',
'X_SyncDestroyAlarm', 'X_SyncSetPriority', 'X_SyncGetPriority',
'XSyncCounterNotify', 'XSyncAlarmNotify', 'XSyncAlarmNotifyMask',
'XSyncNumberEvents', 'XSyncBadCounter', 'XSyncBadAlarm', 'XSyncNumberErrors',
'XSyncCACounter', 'XSyncCAValueType', 'XSyncCAValue', 'XSyncCATestType',
'XSyncCADelta', 'XSyncCAEvents', 'XSyncValueType', 'XSyncAbsolute',
'XSyncRelative', 'XSyncTestType', 'XSyncPositiveTransition',
'XSyncNegativeTransition', 'XSyncPositiveComparison',
'XSyncNegativeComparison', 'XSyncAlarmState', 'XSyncAlarmActive',
'XSyncAlarmInactive', 'XSyncAlarmDestroyed', 'XSyncCounter', 'XSyncAlarm',
'XSyncValue', 'XSyncIntToValue', 'XSyncIntsToValue', 'XSyncValueGreaterThan',
'XSyncValueLessThan', 'XSyncValueGreaterOrEqual', 'XSyncValueLessOrEqual',
'XSyncValueEqual', 'XSyncValueIsNegative', 'XSyncValueIsZero',
'XSyncValueIsPositive', 'XSyncValueLow32', 'XSyncValueHigh32',
'XSyncValueAdd', 'XSyncValueSubtract', 'XSyncMaxValue', 'XSyncMinValue',
'XSyncSystemCounter', 'XSyncTrigger', 'XSyncWaitCondition',
'XSyncAlarmAttributes', 'XSyncCounterNotifyEvent', 'XSyncAlarmNotifyEvent',
'XSyncAlarmError', 'XSyncCounterError', 'XSyncQueryExtension',
'XSyncInitialize', 'XSyncListSystemCounters', 'XSyncFreeSystemCounterList',
'XSyncCreateCounter', 'XSyncSetCounter', 'XSyncChangeCounter',
'XSyncDestroyCounter', 'XSyncQueryCounter', 'XSyncAwait', 'XSyncCreateAlarm',
'XSyncDestroyAlarm', 'XSyncQueryAlarm', 'XSyncChangeAlarm',
'XSyncSetPriority', 'XSyncGetPriority']
