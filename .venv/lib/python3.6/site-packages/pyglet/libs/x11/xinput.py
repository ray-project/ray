'''Wrapper for Xi

Generated with:
tools/genwrappers.py xinput

Do not modify this file.
'''

__docformat__ =  'restructuredtext'
__version__ = '$Id: wrap.py 1694 2008-01-30 23:12:00Z Alex.Holkner $'

import ctypes
from ctypes import *

import pyglet.lib

_lib = pyglet.lib.load_library('Xi')

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

sz_xGetExtensionVersionReq = 8 	# /usr/include/X11/extensions/XI.h:56
sz_xGetExtensionVersionReply = 32 	# /usr/include/X11/extensions/XI.h:57
sz_xListInputDevicesReq = 4 	# /usr/include/X11/extensions/XI.h:58
sz_xListInputDevicesReply = 32 	# /usr/include/X11/extensions/XI.h:59
sz_xOpenDeviceReq = 8 	# /usr/include/X11/extensions/XI.h:60
sz_xOpenDeviceReply = 32 	# /usr/include/X11/extensions/XI.h:61
sz_xCloseDeviceReq = 8 	# /usr/include/X11/extensions/XI.h:62
sz_xSetDeviceModeReq = 8 	# /usr/include/X11/extensions/XI.h:63
sz_xSetDeviceModeReply = 32 	# /usr/include/X11/extensions/XI.h:64
sz_xSelectExtensionEventReq = 12 	# /usr/include/X11/extensions/XI.h:65
sz_xGetSelectedExtensionEventsReq = 8 	# /usr/include/X11/extensions/XI.h:66
sz_xGetSelectedExtensionEventsReply = 32 	# /usr/include/X11/extensions/XI.h:67
sz_xChangeDeviceDontPropagateListReq = 12 	# /usr/include/X11/extensions/XI.h:68
sz_xGetDeviceDontPropagateListReq = 8 	# /usr/include/X11/extensions/XI.h:69
sz_xGetDeviceDontPropagateListReply = 32 	# /usr/include/X11/extensions/XI.h:70
sz_xGetDeviceMotionEventsReq = 16 	# /usr/include/X11/extensions/XI.h:71
sz_xGetDeviceMotionEventsReply = 32 	# /usr/include/X11/extensions/XI.h:72
sz_xChangeKeyboardDeviceReq = 8 	# /usr/include/X11/extensions/XI.h:73
sz_xChangeKeyboardDeviceReply = 32 	# /usr/include/X11/extensions/XI.h:74
sz_xChangePointerDeviceReq = 8 	# /usr/include/X11/extensions/XI.h:75
sz_xChangePointerDeviceReply = 32 	# /usr/include/X11/extensions/XI.h:76
sz_xGrabDeviceReq = 20 	# /usr/include/X11/extensions/XI.h:77
sz_xGrabDeviceReply = 32 	# /usr/include/X11/extensions/XI.h:78
sz_xUngrabDeviceReq = 12 	# /usr/include/X11/extensions/XI.h:79
sz_xGrabDeviceKeyReq = 20 	# /usr/include/X11/extensions/XI.h:80
sz_xGrabDeviceKeyReply = 32 	# /usr/include/X11/extensions/XI.h:81
sz_xUngrabDeviceKeyReq = 16 	# /usr/include/X11/extensions/XI.h:82
sz_xGrabDeviceButtonReq = 20 	# /usr/include/X11/extensions/XI.h:83
sz_xGrabDeviceButtonReply = 32 	# /usr/include/X11/extensions/XI.h:84
sz_xUngrabDeviceButtonReq = 16 	# /usr/include/X11/extensions/XI.h:85
sz_xAllowDeviceEventsReq = 12 	# /usr/include/X11/extensions/XI.h:86
sz_xGetDeviceFocusReq = 8 	# /usr/include/X11/extensions/XI.h:87
sz_xGetDeviceFocusReply = 32 	# /usr/include/X11/extensions/XI.h:88
sz_xSetDeviceFocusReq = 16 	# /usr/include/X11/extensions/XI.h:89
sz_xGetFeedbackControlReq = 8 	# /usr/include/X11/extensions/XI.h:90
sz_xGetFeedbackControlReply = 32 	# /usr/include/X11/extensions/XI.h:91
sz_xChangeFeedbackControlReq = 12 	# /usr/include/X11/extensions/XI.h:92
sz_xGetDeviceKeyMappingReq = 8 	# /usr/include/X11/extensions/XI.h:93
sz_xGetDeviceKeyMappingReply = 32 	# /usr/include/X11/extensions/XI.h:94
sz_xChangeDeviceKeyMappingReq = 8 	# /usr/include/X11/extensions/XI.h:95
sz_xGetDeviceModifierMappingReq = 8 	# /usr/include/X11/extensions/XI.h:96
sz_xSetDeviceModifierMappingReq = 8 	# /usr/include/X11/extensions/XI.h:97
sz_xSetDeviceModifierMappingReply = 32 	# /usr/include/X11/extensions/XI.h:98
sz_xGetDeviceButtonMappingReq = 8 	# /usr/include/X11/extensions/XI.h:99
sz_xGetDeviceButtonMappingReply = 32 	# /usr/include/X11/extensions/XI.h:100
sz_xSetDeviceButtonMappingReq = 8 	# /usr/include/X11/extensions/XI.h:101
sz_xSetDeviceButtonMappingReply = 32 	# /usr/include/X11/extensions/XI.h:102
sz_xQueryDeviceStateReq = 8 	# /usr/include/X11/extensions/XI.h:103
sz_xQueryDeviceStateReply = 32 	# /usr/include/X11/extensions/XI.h:104
sz_xSendExtensionEventReq = 16 	# /usr/include/X11/extensions/XI.h:105
sz_xDeviceBellReq = 8 	# /usr/include/X11/extensions/XI.h:106
sz_xSetDeviceValuatorsReq = 8 	# /usr/include/X11/extensions/XI.h:107
sz_xSetDeviceValuatorsReply = 32 	# /usr/include/X11/extensions/XI.h:108
sz_xGetDeviceControlReq = 8 	# /usr/include/X11/extensions/XI.h:109
sz_xGetDeviceControlReply = 32 	# /usr/include/X11/extensions/XI.h:110
sz_xChangeDeviceControlReq = 8 	# /usr/include/X11/extensions/XI.h:111
sz_xChangeDeviceControlReply = 32 	# /usr/include/X11/extensions/XI.h:112
Dont_Check = 0 	# /usr/include/X11/extensions/XI.h:135
XInput_Initial_Release = 1 	# /usr/include/X11/extensions/XI.h:136
XInput_Add_XDeviceBell = 2 	# /usr/include/X11/extensions/XI.h:137
XInput_Add_XSetDeviceValuators = 3 	# /usr/include/X11/extensions/XI.h:138
XInput_Add_XChangeDeviceControl = 4 	# /usr/include/X11/extensions/XI.h:139
XInput_Add_DevicePresenceNotify = 5 	# /usr/include/X11/extensions/XI.h:140
XI_Absent = 0 	# /usr/include/X11/extensions/XI.h:142
XI_Present = 1 	# /usr/include/X11/extensions/XI.h:143
XI_Initial_Release_Major = 1 	# /usr/include/X11/extensions/XI.h:145
XI_Initial_Release_Minor = 0 	# /usr/include/X11/extensions/XI.h:146
XI_Add_XDeviceBell_Major = 1 	# /usr/include/X11/extensions/XI.h:148
XI_Add_XDeviceBell_Minor = 1 	# /usr/include/X11/extensions/XI.h:149
XI_Add_XSetDeviceValuators_Major = 1 	# /usr/include/X11/extensions/XI.h:151
XI_Add_XSetDeviceValuators_Minor = 2 	# /usr/include/X11/extensions/XI.h:152
XI_Add_XChangeDeviceControl_Major = 1 	# /usr/include/X11/extensions/XI.h:154
XI_Add_XChangeDeviceControl_Minor = 3 	# /usr/include/X11/extensions/XI.h:155
XI_Add_DevicePresenceNotify_Major = 1 	# /usr/include/X11/extensions/XI.h:157
XI_Add_DevicePresenceNotify_Minor = 4 	# /usr/include/X11/extensions/XI.h:158
DEVICE_RESOLUTION = 1 	# /usr/include/X11/extensions/XI.h:160
DEVICE_ABS_CALIB = 2 	# /usr/include/X11/extensions/XI.h:161
DEVICE_CORE = 3 	# /usr/include/X11/extensions/XI.h:162
DEVICE_ENABLE = 4 	# /usr/include/X11/extensions/XI.h:163
DEVICE_ABS_AREA = 5 	# /usr/include/X11/extensions/XI.h:164
NoSuchExtension = 1 	# /usr/include/X11/extensions/XI.h:166
COUNT = 0 	# /usr/include/X11/extensions/XI.h:168
CREATE = 1 	# /usr/include/X11/extensions/XI.h:169
NewPointer = 0 	# /usr/include/X11/extensions/XI.h:171
NewKeyboard = 1 	# /usr/include/X11/extensions/XI.h:172
XPOINTER = 0 	# /usr/include/X11/extensions/XI.h:174
XKEYBOARD = 1 	# /usr/include/X11/extensions/XI.h:175
UseXKeyboard = 255 	# /usr/include/X11/extensions/XI.h:177
IsXPointer = 0 	# /usr/include/X11/extensions/XI.h:179
IsXKeyboard = 1 	# /usr/include/X11/extensions/XI.h:180
IsXExtensionDevice = 2 	# /usr/include/X11/extensions/XI.h:181
IsXExtensionKeyboard = 3 	# /usr/include/X11/extensions/XI.h:182
IsXExtensionPointer = 4 	# /usr/include/X11/extensions/XI.h:183
AsyncThisDevice = 0 	# /usr/include/X11/extensions/XI.h:185
SyncThisDevice = 1 	# /usr/include/X11/extensions/XI.h:186
ReplayThisDevice = 2 	# /usr/include/X11/extensions/XI.h:187
AsyncOtherDevices = 3 	# /usr/include/X11/extensions/XI.h:188
AsyncAll = 4 	# /usr/include/X11/extensions/XI.h:189
SyncAll = 5 	# /usr/include/X11/extensions/XI.h:190
FollowKeyboard = 3 	# /usr/include/X11/extensions/XI.h:192
RevertToFollowKeyboard = 3 	# /usr/include/X11/extensions/XI.h:194
DvAccelNum = 1 	# /usr/include/X11/extensions/XI.h:197
DvAccelDenom = 2 	# /usr/include/X11/extensions/XI.h:198
DvThreshold = 4 	# /usr/include/X11/extensions/XI.h:199
DvKeyClickPercent = 1 	# /usr/include/X11/extensions/XI.h:201
DvPercent = 2 	# /usr/include/X11/extensions/XI.h:202
DvPitch = 4 	# /usr/include/X11/extensions/XI.h:203
DvDuration = 8 	# /usr/include/X11/extensions/XI.h:204
DvLed = 16 	# /usr/include/X11/extensions/XI.h:205
DvLedMode = 32 	# /usr/include/X11/extensions/XI.h:206
DvKey = 64 	# /usr/include/X11/extensions/XI.h:207
DvAutoRepeatMode = 128 	# /usr/include/X11/extensions/XI.h:208
DvString = 1 	# /usr/include/X11/extensions/XI.h:210
DvInteger = 1 	# /usr/include/X11/extensions/XI.h:212
DeviceMode = 1 	# /usr/include/X11/extensions/XI.h:214
Relative = 0 	# /usr/include/X11/extensions/XI.h:215
Absolute = 1 	# /usr/include/X11/extensions/XI.h:216
ProximityState = 2 	# /usr/include/X11/extensions/XI.h:218
InProximity = 0 	# /usr/include/X11/extensions/XI.h:219
OutOfProximity = 2 	# /usr/include/X11/extensions/XI.h:220
AddToList = 0 	# /usr/include/X11/extensions/XI.h:222
DeleteFromList = 1 	# /usr/include/X11/extensions/XI.h:223
KeyClass = 0 	# /usr/include/X11/extensions/XI.h:225
ButtonClass = 1 	# /usr/include/X11/extensions/XI.h:226
ValuatorClass = 2 	# /usr/include/X11/extensions/XI.h:227
FeedbackClass = 3 	# /usr/include/X11/extensions/XI.h:228
ProximityClass = 4 	# /usr/include/X11/extensions/XI.h:229
FocusClass = 5 	# /usr/include/X11/extensions/XI.h:230
OtherClass = 6 	# /usr/include/X11/extensions/XI.h:231
KbdFeedbackClass = 0 	# /usr/include/X11/extensions/XI.h:233
PtrFeedbackClass = 1 	# /usr/include/X11/extensions/XI.h:234
StringFeedbackClass = 2 	# /usr/include/X11/extensions/XI.h:235
IntegerFeedbackClass = 3 	# /usr/include/X11/extensions/XI.h:236
LedFeedbackClass = 4 	# /usr/include/X11/extensions/XI.h:237
BellFeedbackClass = 5 	# /usr/include/X11/extensions/XI.h:238
_devicePointerMotionHint = 0 	# /usr/include/X11/extensions/XI.h:240
_deviceButton1Motion = 1 	# /usr/include/X11/extensions/XI.h:241
_deviceButton2Motion = 2 	# /usr/include/X11/extensions/XI.h:242
_deviceButton3Motion = 3 	# /usr/include/X11/extensions/XI.h:243
_deviceButton4Motion = 4 	# /usr/include/X11/extensions/XI.h:244
_deviceButton5Motion = 5 	# /usr/include/X11/extensions/XI.h:245
_deviceButtonMotion = 6 	# /usr/include/X11/extensions/XI.h:246
_deviceButtonGrab = 7 	# /usr/include/X11/extensions/XI.h:247
_deviceOwnerGrabButton = 8 	# /usr/include/X11/extensions/XI.h:248
_noExtensionEvent = 9 	# /usr/include/X11/extensions/XI.h:249
_devicePresence = 0 	# /usr/include/X11/extensions/XI.h:251
DeviceAdded = 0 	# /usr/include/X11/extensions/XI.h:253
DeviceRemoved = 1 	# /usr/include/X11/extensions/XI.h:254
DeviceEnabled = 2 	# /usr/include/X11/extensions/XI.h:255
DeviceDisabled = 3 	# /usr/include/X11/extensions/XI.h:256
DeviceUnrecoverable = 4 	# /usr/include/X11/extensions/XI.h:257
XI_BadDevice = 0 	# /usr/include/X11/extensions/XI.h:259
XI_BadEvent = 1 	# /usr/include/X11/extensions/XI.h:260
XI_BadMode = 2 	# /usr/include/X11/extensions/XI.h:261
XI_DeviceBusy = 3 	# /usr/include/X11/extensions/XI.h:262
XI_BadClass = 4 	# /usr/include/X11/extensions/XI.h:263
XEventClass = c_ulong 	# /usr/include/X11/extensions/XI.h:272
class struct_anon_93(Structure):
    __slots__ = [
        'present',
        'major_version',
        'minor_version',
    ]
struct_anon_93._fields_ = [
    ('present', c_int),
    ('major_version', c_short),
    ('minor_version', c_short),
]

XExtensionVersion = struct_anon_93 	# /usr/include/X11/extensions/XI.h:285
_deviceKeyPress = 0 	# /usr/include/X11/extensions/XInput.h:4902
_deviceKeyRelease = 1 	# /usr/include/X11/extensions/XInput.h:4903
_deviceButtonPress = 0 	# /usr/include/X11/extensions/XInput.h:4905
_deviceButtonRelease = 1 	# /usr/include/X11/extensions/XInput.h:4906
_deviceMotionNotify = 0 	# /usr/include/X11/extensions/XInput.h:4908
_deviceFocusIn = 0 	# /usr/include/X11/extensions/XInput.h:4910
_deviceFocusOut = 1 	# /usr/include/X11/extensions/XInput.h:4911
_proximityIn = 0 	# /usr/include/X11/extensions/XInput.h:4913
_proximityOut = 1 	# /usr/include/X11/extensions/XInput.h:4914
_deviceStateNotify = 0 	# /usr/include/X11/extensions/XInput.h:4916
_deviceMappingNotify = 1 	# /usr/include/X11/extensions/XInput.h:4917
_changeDeviceNotify = 2 	# /usr/include/X11/extensions/XInput.h:4918
class struct_anon_94(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'keycode',
        'same_screen',
        'device_state',
        'axes_count',
        'first_axis',
        'axis_data',
    ]
Display = pyglet.libs.x11.xlib.Display
Window = pyglet.libs.x11.xlib.Window
XID = pyglet.libs.x11.xlib.XID
Time = pyglet.libs.x11.xlib.Time
struct_anon_94._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('keycode', c_uint),
    ('same_screen', c_int),
    ('device_state', c_uint),
    ('axes_count', c_ubyte),
    ('first_axis', c_ubyte),
    ('axis_data', c_int * 6),
]

XDeviceKeyEvent = struct_anon_94 	# /usr/include/X11/extensions/XInput.h:5043
XDeviceKeyPressedEvent = XDeviceKeyEvent 	# /usr/include/X11/extensions/XInput.h:5045
XDeviceKeyReleasedEvent = XDeviceKeyEvent 	# /usr/include/X11/extensions/XInput.h:5046
class struct_anon_95(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'button',
        'same_screen',
        'device_state',
        'axes_count',
        'first_axis',
        'axis_data',
    ]
struct_anon_95._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('button', c_uint),
    ('same_screen', c_int),
    ('device_state', c_uint),
    ('axes_count', c_ubyte),
    ('first_axis', c_ubyte),
    ('axis_data', c_int * 6),
]

XDeviceButtonEvent = struct_anon_95 	# /usr/include/X11/extensions/XInput.h:5075
XDeviceButtonPressedEvent = XDeviceButtonEvent 	# /usr/include/X11/extensions/XInput.h:5077
XDeviceButtonReleasedEvent = XDeviceButtonEvent 	# /usr/include/X11/extensions/XInput.h:5078
class struct_anon_96(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'is_hint',
        'same_screen',
        'device_state',
        'axes_count',
        'first_axis',
        'axis_data',
    ]
struct_anon_96._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('is_hint', c_char),
    ('same_screen', c_int),
    ('device_state', c_uint),
    ('axes_count', c_ubyte),
    ('first_axis', c_ubyte),
    ('axis_data', c_int * 6),
]

XDeviceMotionEvent = struct_anon_96 	# /usr/include/X11/extensions/XInput.h:5108
class struct_anon_97(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'mode',
        'detail',
        'time',
    ]
struct_anon_97._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('mode', c_int),
    ('detail', c_int),
    ('time', Time),
]

XDeviceFocusChangeEvent = struct_anon_97 	# /usr/include/X11/extensions/XInput.h:5133
XDeviceFocusInEvent = XDeviceFocusChangeEvent 	# /usr/include/X11/extensions/XInput.h:5135
XDeviceFocusOutEvent = XDeviceFocusChangeEvent 	# /usr/include/X11/extensions/XInput.h:5136
class struct_anon_98(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'root',
        'subwindow',
        'time',
        'x',
        'y',
        'x_root',
        'y_root',
        'state',
        'same_screen',
        'device_state',
        'axes_count',
        'first_axis',
        'axis_data',
    ]
struct_anon_98._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('root', Window),
    ('subwindow', Window),
    ('time', Time),
    ('x', c_int),
    ('y', c_int),
    ('x_root', c_int),
    ('y_root', c_int),
    ('state', c_uint),
    ('same_screen', c_int),
    ('device_state', c_uint),
    ('axes_count', c_ubyte),
    ('first_axis', c_ubyte),
    ('axis_data', c_int * 6),
]

XProximityNotifyEvent = struct_anon_98 	# /usr/include/X11/extensions/XInput.h:5164
XProximityInEvent = XProximityNotifyEvent 	# /usr/include/X11/extensions/XInput.h:5165
XProximityOutEvent = XProximityNotifyEvent 	# /usr/include/X11/extensions/XInput.h:5166
class struct_anon_99(Structure):
    __slots__ = [
        'class',
        'length',
    ]
struct_anon_99._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
]

XInputClass = struct_anon_99 	# /usr/include/X11/extensions/XInput.h:5183
class struct_anon_100(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'time',
        'num_classes',
        'data',
    ]
struct_anon_100._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('time', Time),
    ('num_classes', c_int),
    ('data', c_char * 64),
]

XDeviceStateNotifyEvent = struct_anon_100 	# /usr/include/X11/extensions/XInput.h:5195
class struct_anon_101(Structure):
    __slots__ = [
        'class',
        'length',
        'num_valuators',
        'mode',
        'valuators',
    ]
struct_anon_101._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
    ('num_valuators', c_ubyte),
    ('mode', c_ubyte),
    ('valuators', c_int * 6),
]

XValuatorStatus = struct_anon_101 	# /usr/include/X11/extensions/XInput.h:5207
class struct_anon_102(Structure):
    __slots__ = [
        'class',
        'length',
        'num_keys',
        'keys',
    ]
struct_anon_102._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
    ('num_keys', c_short),
    ('keys', c_char * 32),
]

XKeyStatus = struct_anon_102 	# /usr/include/X11/extensions/XInput.h:5218
class struct_anon_103(Structure):
    __slots__ = [
        'class',
        'length',
        'num_buttons',
        'buttons',
    ]
struct_anon_103._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
    ('num_buttons', c_short),
    ('buttons', c_char * 32),
]

XButtonStatus = struct_anon_103 	# /usr/include/X11/extensions/XInput.h:5229
class struct_anon_104(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'time',
        'request',
        'first_keycode',
        'count',
    ]
struct_anon_104._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('time', Time),
    ('request', c_int),
    ('first_keycode', c_int),
    ('count', c_int),
]

XDeviceMappingEvent = struct_anon_104 	# /usr/include/X11/extensions/XInput.h:5250
class struct_anon_105(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'deviceid',
        'time',
        'request',
    ]
struct_anon_105._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('deviceid', XID),
    ('time', Time),
    ('request', c_int),
]

XChangeDeviceNotifyEvent = struct_anon_105 	# /usr/include/X11/extensions/XInput.h:5268
class struct_anon_106(Structure):
    __slots__ = [
        'type',
        'serial',
        'send_event',
        'display',
        'window',
        'time',
        'devchange',
        'deviceid',
        'control',
    ]
struct_anon_106._fields_ = [
    ('type', c_int),
    ('serial', c_ulong),
    ('send_event', c_int),
    ('display', POINTER(Display)),
    ('window', Window),
    ('time', Time),
    ('devchange', c_int),
    ('deviceid', XID),
    ('control', XID),
]

XDevicePresenceNotifyEvent = struct_anon_106 	# /usr/include/X11/extensions/XInput.h:5293
class struct_anon_107(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
    ]
struct_anon_107._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
]

XFeedbackState = struct_anon_107 	# /usr/include/X11/extensions/XInput.h:5311
class struct_anon_108(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'click',
        'percent',
        'pitch',
        'duration',
        'led_mask',
        'global_auto_repeat',
        'auto_repeats',
    ]
struct_anon_108._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('click', c_int),
    ('percent', c_int),
    ('pitch', c_int),
    ('duration', c_int),
    ('led_mask', c_int),
    ('global_auto_repeat', c_int),
    ('auto_repeats', c_char * 32),
]

XKbdFeedbackState = struct_anon_108 	# /usr/include/X11/extensions/XInput.h:5328
class struct_anon_109(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'accelNum',
        'accelDenom',
        'threshold',
    ]
struct_anon_109._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('accelNum', c_int),
    ('accelDenom', c_int),
    ('threshold', c_int),
]

XPtrFeedbackState = struct_anon_109 	# /usr/include/X11/extensions/XInput.h:5341
class struct_anon_110(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'resolution',
        'minVal',
        'maxVal',
    ]
struct_anon_110._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('resolution', c_int),
    ('minVal', c_int),
    ('maxVal', c_int),
]

XIntegerFeedbackState = struct_anon_110 	# /usr/include/X11/extensions/XInput.h:5354
class struct_anon_111(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'max_symbols',
        'num_syms_supported',
        'syms_supported',
    ]
KeySym = pyglet.libs.x11.xlib.KeySym
struct_anon_111._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('max_symbols', c_int),
    ('num_syms_supported', c_int),
    ('syms_supported', POINTER(KeySym)),
]

XStringFeedbackState = struct_anon_111 	# /usr/include/X11/extensions/XInput.h:5367
class struct_anon_112(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'percent',
        'pitch',
        'duration',
    ]
struct_anon_112._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('percent', c_int),
    ('pitch', c_int),
    ('duration', c_int),
]

XBellFeedbackState = struct_anon_112 	# /usr/include/X11/extensions/XInput.h:5380
class struct_anon_113(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'led_values',
        'led_mask',
    ]
struct_anon_113._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('led_values', c_int),
    ('led_mask', c_int),
]

XLedFeedbackState = struct_anon_113 	# /usr/include/X11/extensions/XInput.h:5392
class struct_anon_114(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
    ]
struct_anon_114._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
]

XFeedbackControl = struct_anon_114 	# /usr/include/X11/extensions/XInput.h:5402
class struct_anon_115(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'accelNum',
        'accelDenom',
        'threshold',
    ]
struct_anon_115._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('accelNum', c_int),
    ('accelDenom', c_int),
    ('threshold', c_int),
]

XPtrFeedbackControl = struct_anon_115 	# /usr/include/X11/extensions/XInput.h:5415
class struct_anon_116(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'click',
        'percent',
        'pitch',
        'duration',
        'led_mask',
        'led_value',
        'key',
        'auto_repeat_mode',
    ]
struct_anon_116._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('click', c_int),
    ('percent', c_int),
    ('pitch', c_int),
    ('duration', c_int),
    ('led_mask', c_int),
    ('led_value', c_int),
    ('key', c_int),
    ('auto_repeat_mode', c_int),
]

XKbdFeedbackControl = struct_anon_116 	# /usr/include/X11/extensions/XInput.h:5433
class struct_anon_117(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'num_keysyms',
        'syms_to_display',
    ]
struct_anon_117._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('num_keysyms', c_int),
    ('syms_to_display', POINTER(KeySym)),
]

XStringFeedbackControl = struct_anon_117 	# /usr/include/X11/extensions/XInput.h:5445
class struct_anon_118(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'int_to_display',
    ]
struct_anon_118._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('int_to_display', c_int),
]

XIntegerFeedbackControl = struct_anon_118 	# /usr/include/X11/extensions/XInput.h:5456
class struct_anon_119(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'percent',
        'pitch',
        'duration',
    ]
struct_anon_119._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('percent', c_int),
    ('pitch', c_int),
    ('duration', c_int),
]

XBellFeedbackControl = struct_anon_119 	# /usr/include/X11/extensions/XInput.h:5469
class struct_anon_120(Structure):
    __slots__ = [
        'class',
        'length',
        'id',
        'led_mask',
        'led_values',
    ]
struct_anon_120._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('id', XID),
    ('led_mask', c_int),
    ('led_values', c_int),
]

XLedFeedbackControl = struct_anon_120 	# /usr/include/X11/extensions/XInput.h:5481
class struct_anon_121(Structure):
    __slots__ = [
        'control',
        'length',
    ]
struct_anon_121._fields_ = [
    ('control', XID),
    ('length', c_int),
]

XDeviceControl = struct_anon_121 	# /usr/include/X11/extensions/XInput.h:5492
class struct_anon_122(Structure):
    __slots__ = [
        'control',
        'length',
        'first_valuator',
        'num_valuators',
        'resolutions',
    ]
struct_anon_122._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('first_valuator', c_int),
    ('num_valuators', c_int),
    ('resolutions', POINTER(c_int)),
]

XDeviceResolutionControl = struct_anon_122 	# /usr/include/X11/extensions/XInput.h:5500
class struct_anon_123(Structure):
    __slots__ = [
        'control',
        'length',
        'num_valuators',
        'resolutions',
        'min_resolutions',
        'max_resolutions',
    ]
struct_anon_123._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('num_valuators', c_int),
    ('resolutions', POINTER(c_int)),
    ('min_resolutions', POINTER(c_int)),
    ('max_resolutions', POINTER(c_int)),
]

XDeviceResolutionState = struct_anon_123 	# /usr/include/X11/extensions/XInput.h:5509
class struct_anon_124(Structure):
    __slots__ = [
        'control',
        'length',
        'min_x',
        'max_x',
        'min_y',
        'max_y',
        'flip_x',
        'flip_y',
        'rotation',
        'button_threshold',
    ]
struct_anon_124._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('min_x', c_int),
    ('max_x', c_int),
    ('min_y', c_int),
    ('max_y', c_int),
    ('flip_x', c_int),
    ('flip_y', c_int),
    ('rotation', c_int),
    ('button_threshold', c_int),
]

XDeviceAbsCalibControl = struct_anon_124 	# /usr/include/X11/extensions/XInput.h:5522
class struct_anon_125(Structure):
    __slots__ = [
        'control',
        'length',
        'min_x',
        'max_x',
        'min_y',
        'max_y',
        'flip_x',
        'flip_y',
        'rotation',
        'button_threshold',
    ]
struct_anon_125._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('min_x', c_int),
    ('max_x', c_int),
    ('min_y', c_int),
    ('max_y', c_int),
    ('flip_x', c_int),
    ('flip_y', c_int),
    ('rotation', c_int),
    ('button_threshold', c_int),
]

XDeviceAbsCalibState = struct_anon_125 	# /usr/include/X11/extensions/XInput.h:5522
class struct_anon_126(Structure):
    __slots__ = [
        'control',
        'length',
        'offset_x',
        'offset_y',
        'width',
        'height',
        'screen',
        'following',
    ]
struct_anon_126._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('offset_x', c_int),
    ('offset_y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('screen', c_int),
    ('following', XID),
]

XDeviceAbsAreaControl = struct_anon_126 	# /usr/include/X11/extensions/XInput.h:5533
class struct_anon_127(Structure):
    __slots__ = [
        'control',
        'length',
        'offset_x',
        'offset_y',
        'width',
        'height',
        'screen',
        'following',
    ]
struct_anon_127._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('offset_x', c_int),
    ('offset_y', c_int),
    ('width', c_int),
    ('height', c_int),
    ('screen', c_int),
    ('following', XID),
]

XDeviceAbsAreaState = struct_anon_127 	# /usr/include/X11/extensions/XInput.h:5533
class struct_anon_128(Structure):
    __slots__ = [
        'control',
        'length',
        'status',
    ]
struct_anon_128._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('status', c_int),
]

XDeviceCoreControl = struct_anon_128 	# /usr/include/X11/extensions/XInput.h:5539
class struct_anon_129(Structure):
    __slots__ = [
        'control',
        'length',
        'status',
        'iscore',
    ]
struct_anon_129._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('status', c_int),
    ('iscore', c_int),
]

XDeviceCoreState = struct_anon_129 	# /usr/include/X11/extensions/XInput.h:5546
class struct_anon_130(Structure):
    __slots__ = [
        'control',
        'length',
        'enable',
    ]
struct_anon_130._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('enable', c_int),
]

XDeviceEnableControl = struct_anon_130 	# /usr/include/X11/extensions/XInput.h:5552
class struct_anon_131(Structure):
    __slots__ = [
        'control',
        'length',
        'enable',
    ]
struct_anon_131._fields_ = [
    ('control', XID),
    ('length', c_int),
    ('enable', c_int),
]

XDeviceEnableState = struct_anon_131 	# /usr/include/X11/extensions/XInput.h:5552
class struct__XAnyClassinfo(Structure):
    __slots__ = [
    ]
struct__XAnyClassinfo._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XAnyClassinfo(Structure):
    __slots__ = [
    ]
struct__XAnyClassinfo._fields_ = [
    ('_opaque_struct', c_int)
]

XAnyClassPtr = POINTER(struct__XAnyClassinfo) 	# /usr/include/X11/extensions/XInput.h:5564
class struct__XAnyClassinfo(Structure):
    __slots__ = [
        'class',
        'length',
    ]
struct__XAnyClassinfo._fields_ = [
    ('class', XID),
    ('length', c_int),
]

XAnyClassInfo = struct__XAnyClassinfo 	# /usr/include/X11/extensions/XInput.h:5573
class struct__XDeviceInfo(Structure):
    __slots__ = [
    ]
struct__XDeviceInfo._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XDeviceInfo(Structure):
    __slots__ = [
    ]
struct__XDeviceInfo._fields_ = [
    ('_opaque_struct', c_int)
]

XDeviceInfoPtr = POINTER(struct__XDeviceInfo) 	# /usr/include/X11/extensions/XInput.h:5575
class struct__XDeviceInfo(Structure):
    __slots__ = [
        'id',
        'type',
        'name',
        'num_classes',
        'use',
        'inputclassinfo',
    ]
Atom = pyglet.libs.x11.xlib.Atom
struct__XDeviceInfo._fields_ = [
    ('id', XID),
    ('type', Atom),
    ('name', c_char_p),
    ('num_classes', c_int),
    ('use', c_int),
    ('inputclassinfo', XAnyClassPtr),
]

XDeviceInfo = struct__XDeviceInfo 	# /usr/include/X11/extensions/XInput.h:5585
class struct__XKeyInfo(Structure):
    __slots__ = [
    ]
struct__XKeyInfo._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XKeyInfo(Structure):
    __slots__ = [
    ]
struct__XKeyInfo._fields_ = [
    ('_opaque_struct', c_int)
]

XKeyInfoPtr = POINTER(struct__XKeyInfo) 	# /usr/include/X11/extensions/XInput.h:5587
class struct__XKeyInfo(Structure):
    __slots__ = [
        'class',
        'length',
        'min_keycode',
        'max_keycode',
        'num_keys',
    ]
struct__XKeyInfo._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('min_keycode', c_ushort),
    ('max_keycode', c_ushort),
    ('num_keys', c_ushort),
]

XKeyInfo = struct__XKeyInfo 	# /usr/include/X11/extensions/XInput.h:5600
class struct__XButtonInfo(Structure):
    __slots__ = [
    ]
struct__XButtonInfo._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XButtonInfo(Structure):
    __slots__ = [
    ]
struct__XButtonInfo._fields_ = [
    ('_opaque_struct', c_int)
]

XButtonInfoPtr = POINTER(struct__XButtonInfo) 	# /usr/include/X11/extensions/XInput.h:5602
class struct__XButtonInfo(Structure):
    __slots__ = [
        'class',
        'length',
        'num_buttons',
    ]
struct__XButtonInfo._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('num_buttons', c_short),
]

XButtonInfo = struct__XButtonInfo 	# /usr/include/X11/extensions/XInput.h:5612
class struct__XAxisInfo(Structure):
    __slots__ = [
    ]
struct__XAxisInfo._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XAxisInfo(Structure):
    __slots__ = [
    ]
struct__XAxisInfo._fields_ = [
    ('_opaque_struct', c_int)
]

XAxisInfoPtr = POINTER(struct__XAxisInfo) 	# /usr/include/X11/extensions/XInput.h:5614
class struct__XAxisInfo(Structure):
    __slots__ = [
        'resolution',
        'min_value',
        'max_value',
    ]
struct__XAxisInfo._fields_ = [
    ('resolution', c_int),
    ('min_value', c_int),
    ('max_value', c_int),
]

XAxisInfo = struct__XAxisInfo 	# /usr/include/X11/extensions/XInput.h:5620
class struct__XValuatorInfo(Structure):
    __slots__ = [
    ]
struct__XValuatorInfo._fields_ = [
    ('_opaque_struct', c_int)
]

class struct__XValuatorInfo(Structure):
    __slots__ = [
    ]
struct__XValuatorInfo._fields_ = [
    ('_opaque_struct', c_int)
]

XValuatorInfoPtr = POINTER(struct__XValuatorInfo) 	# /usr/include/X11/extensions/XInput.h:5622
class struct__XValuatorInfo(Structure):
    __slots__ = [
        'class',
        'length',
        'num_axes',
        'mode',
        'motion_buffer',
        'axes',
    ]
struct__XValuatorInfo._fields_ = [
    ('class', XID),
    ('length', c_int),
    ('num_axes', c_ubyte),
    ('mode', c_ubyte),
    ('motion_buffer', c_ulong),
    ('axes', XAxisInfoPtr),
]

XValuatorInfo = struct__XValuatorInfo 	# /usr/include/X11/extensions/XInput.h:5636
class struct_anon_132(Structure):
    __slots__ = [
        'input_class',
        'event_type_base',
    ]
struct_anon_132._fields_ = [
    ('input_class', c_ubyte),
    ('event_type_base', c_ubyte),
]

XInputClassInfo = struct_anon_132 	# /usr/include/X11/extensions/XInput.h:5653
class struct_anon_133(Structure):
    __slots__ = [
        'device_id',
        'num_classes',
        'classes',
    ]
struct_anon_133._fields_ = [
    ('device_id', XID),
    ('num_classes', c_int),
    ('classes', POINTER(XInputClassInfo)),
]

XDevice = struct_anon_133 	# /usr/include/X11/extensions/XInput.h:5659
class struct_anon_134(Structure):
    __slots__ = [
        'event_type',
        'device',
    ]
struct_anon_134._fields_ = [
    ('event_type', XEventClass),
    ('device', XID),
]

XEventList = struct_anon_134 	# /usr/include/X11/extensions/XInput.h:5672
class struct_anon_135(Structure):
    __slots__ = [
        'time',
        'data',
    ]
struct_anon_135._fields_ = [
    ('time', Time),
    ('data', POINTER(c_int)),
]

XDeviceTimeCoord = struct_anon_135 	# /usr/include/X11/extensions/XInput.h:5685
class struct_anon_136(Structure):
    __slots__ = [
        'device_id',
        'num_classes',
        'data',
    ]
struct_anon_136._fields_ = [
    ('device_id', XID),
    ('num_classes', c_int),
    ('data', POINTER(XInputClass)),
]

XDeviceState = struct_anon_136 	# /usr/include/X11/extensions/XInput.h:5699
class struct_anon_137(Structure):
    __slots__ = [
        'class',
        'length',
        'num_valuators',
        'mode',
        'valuators',
    ]
struct_anon_137._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
    ('num_valuators', c_ubyte),
    ('mode', c_ubyte),
    ('valuators', POINTER(c_int)),
]

XValuatorState = struct_anon_137 	# /usr/include/X11/extensions/XInput.h:5722
class struct_anon_138(Structure):
    __slots__ = [
        'class',
        'length',
        'num_keys',
        'keys',
    ]
struct_anon_138._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
    ('num_keys', c_short),
    ('keys', c_char * 32),
]

XKeyState = struct_anon_138 	# /usr/include/X11/extensions/XInput.h:5733
class struct_anon_139(Structure):
    __slots__ = [
        'class',
        'length',
        'num_buttons',
        'buttons',
    ]
struct_anon_139._fields_ = [
    ('class', c_ubyte),
    ('length', c_ubyte),
    ('num_buttons', c_short),
    ('buttons', c_char * 32),
]

XButtonState = struct_anon_139 	# /usr/include/X11/extensions/XInput.h:5744
# /usr/include/X11/extensions/XInput.h:5754
XChangeKeyboardDevice = _lib.XChangeKeyboardDevice
XChangeKeyboardDevice.restype = c_int
XChangeKeyboardDevice.argtypes = [POINTER(Display), POINTER(XDevice)]

# /usr/include/X11/extensions/XInput.h:5759
XChangePointerDevice = _lib.XChangePointerDevice
XChangePointerDevice.restype = c_int
XChangePointerDevice.argtypes = [POINTER(Display), POINTER(XDevice), c_int, c_int]

# /usr/include/X11/extensions/XInput.h:5766
XGrabDevice = _lib.XGrabDevice
XGrabDevice.restype = c_int
XGrabDevice.argtypes = [POINTER(Display), POINTER(XDevice), Window, c_int, c_int, POINTER(XEventClass), c_int, c_int, Time]

# /usr/include/X11/extensions/XInput.h:5778
XUngrabDevice = _lib.XUngrabDevice
XUngrabDevice.restype = c_int
XUngrabDevice.argtypes = [POINTER(Display), POINTER(XDevice), Time]

# /usr/include/X11/extensions/XInput.h:5784
XGrabDeviceKey = _lib.XGrabDeviceKey
XGrabDeviceKey.restype = c_int
XGrabDeviceKey.argtypes = [POINTER(Display), POINTER(XDevice), c_uint, c_uint, POINTER(XDevice), Window, c_int, c_uint, POINTER(XEventClass), c_int, c_int]

# /usr/include/X11/extensions/XInput.h:5798
XUngrabDeviceKey = _lib.XUngrabDeviceKey
XUngrabDeviceKey.restype = c_int
XUngrabDeviceKey.argtypes = [POINTER(Display), POINTER(XDevice), c_uint, c_uint, POINTER(XDevice), Window]

# /usr/include/X11/extensions/XInput.h:5807
XGrabDeviceButton = _lib.XGrabDeviceButton
XGrabDeviceButton.restype = c_int
XGrabDeviceButton.argtypes = [POINTER(Display), POINTER(XDevice), c_uint, c_uint, POINTER(XDevice), Window, c_int, c_uint, POINTER(XEventClass), c_int, c_int]

# /usr/include/X11/extensions/XInput.h:5821
XUngrabDeviceButton = _lib.XUngrabDeviceButton
XUngrabDeviceButton.restype = c_int
XUngrabDeviceButton.argtypes = [POINTER(Display), POINTER(XDevice), c_uint, c_uint, POINTER(XDevice), Window]

# /usr/include/X11/extensions/XInput.h:5830
XAllowDeviceEvents = _lib.XAllowDeviceEvents
XAllowDeviceEvents.restype = c_int
XAllowDeviceEvents.argtypes = [POINTER(Display), POINTER(XDevice), c_int, Time]

# /usr/include/X11/extensions/XInput.h:5837
XGetDeviceFocus = _lib.XGetDeviceFocus
XGetDeviceFocus.restype = c_int
XGetDeviceFocus.argtypes = [POINTER(Display), POINTER(XDevice), POINTER(Window), POINTER(c_int), POINTER(Time)]

# /usr/include/X11/extensions/XInput.h:5845
XSetDeviceFocus = _lib.XSetDeviceFocus
XSetDeviceFocus.restype = c_int
XSetDeviceFocus.argtypes = [POINTER(Display), POINTER(XDevice), Window, c_int, Time]

# /usr/include/X11/extensions/XInput.h:5853
XGetFeedbackControl = _lib.XGetFeedbackControl
XGetFeedbackControl.restype = POINTER(XFeedbackState)
XGetFeedbackControl.argtypes = [POINTER(Display), POINTER(XDevice), POINTER(c_int)]

# /usr/include/X11/extensions/XInput.h:5859
XFreeFeedbackList = _lib.XFreeFeedbackList
XFreeFeedbackList.restype = None
XFreeFeedbackList.argtypes = [POINTER(XFeedbackState)]

# /usr/include/X11/extensions/XInput.h:5863
XChangeFeedbackControl = _lib.XChangeFeedbackControl
XChangeFeedbackControl.restype = c_int
XChangeFeedbackControl.argtypes = [POINTER(Display), POINTER(XDevice), c_ulong, POINTER(XFeedbackControl)]

# /usr/include/X11/extensions/XInput.h:5870
XDeviceBell = _lib.XDeviceBell
XDeviceBell.restype = c_int
XDeviceBell.argtypes = [POINTER(Display), POINTER(XDevice), XID, XID, c_int]

KeyCode = pyglet.libs.x11.xlib.KeyCode
# /usr/include/X11/extensions/XInput.h:5878
XGetDeviceKeyMapping = _lib.XGetDeviceKeyMapping
XGetDeviceKeyMapping.restype = POINTER(KeySym)
XGetDeviceKeyMapping.argtypes = [POINTER(Display), POINTER(XDevice), KeyCode, c_int, POINTER(c_int)]

# /usr/include/X11/extensions/XInput.h:5890
XChangeDeviceKeyMapping = _lib.XChangeDeviceKeyMapping
XChangeDeviceKeyMapping.restype = c_int
XChangeDeviceKeyMapping.argtypes = [POINTER(Display), POINTER(XDevice), c_int, c_int, POINTER(KeySym), c_int]

XModifierKeymap = pyglet.libs.x11.xlib.XModifierKeymap
# /usr/include/X11/extensions/XInput.h:5899
XGetDeviceModifierMapping = _lib.XGetDeviceModifierMapping
XGetDeviceModifierMapping.restype = POINTER(XModifierKeymap)
XGetDeviceModifierMapping.argtypes = [POINTER(Display), POINTER(XDevice)]

# /usr/include/X11/extensions/XInput.h:5904
XSetDeviceModifierMapping = _lib.XSetDeviceModifierMapping
XSetDeviceModifierMapping.restype = c_int
XSetDeviceModifierMapping.argtypes = [POINTER(Display), POINTER(XDevice), POINTER(XModifierKeymap)]

# /usr/include/X11/extensions/XInput.h:5910
XSetDeviceButtonMapping = _lib.XSetDeviceButtonMapping
XSetDeviceButtonMapping.restype = c_int
XSetDeviceButtonMapping.argtypes = [POINTER(Display), POINTER(XDevice), POINTER(c_ubyte), c_int]

# /usr/include/X11/extensions/XInput.h:5917
XGetDeviceButtonMapping = _lib.XGetDeviceButtonMapping
XGetDeviceButtonMapping.restype = c_int
XGetDeviceButtonMapping.argtypes = [POINTER(Display), POINTER(XDevice), POINTER(c_ubyte), c_uint]

# /usr/include/X11/extensions/XInput.h:5924
XQueryDeviceState = _lib.XQueryDeviceState
XQueryDeviceState.restype = POINTER(XDeviceState)
XQueryDeviceState.argtypes = [POINTER(Display), POINTER(XDevice)]

# /usr/include/X11/extensions/XInput.h:5929
XFreeDeviceState = _lib.XFreeDeviceState
XFreeDeviceState.restype = None
XFreeDeviceState.argtypes = [POINTER(XDeviceState)]

# /usr/include/X11/extensions/XInput.h:5933
XGetExtensionVersion = _lib.XGetExtensionVersion
XGetExtensionVersion.restype = POINTER(XExtensionVersion)
XGetExtensionVersion.argtypes = [POINTER(Display), c_char_p]

# /usr/include/X11/extensions/XInput.h:5938
XListInputDevices = _lib.XListInputDevices
XListInputDevices.restype = POINTER(XDeviceInfo)
XListInputDevices.argtypes = [POINTER(Display), POINTER(c_int)]

# /usr/include/X11/extensions/XInput.h:5943
XFreeDeviceList = _lib.XFreeDeviceList
XFreeDeviceList.restype = None
XFreeDeviceList.argtypes = [POINTER(XDeviceInfo)]

# /usr/include/X11/extensions/XInput.h:5947
XOpenDevice = _lib.XOpenDevice
XOpenDevice.restype = POINTER(XDevice)
XOpenDevice.argtypes = [POINTER(Display), XID]

# /usr/include/X11/extensions/XInput.h:5952
XCloseDevice = _lib.XCloseDevice
XCloseDevice.restype = c_int
XCloseDevice.argtypes = [POINTER(Display), POINTER(XDevice)]

# /usr/include/X11/extensions/XInput.h:5957
XSetDeviceMode = _lib.XSetDeviceMode
XSetDeviceMode.restype = c_int
XSetDeviceMode.argtypes = [POINTER(Display), POINTER(XDevice), c_int]

# /usr/include/X11/extensions/XInput.h:5963
XSetDeviceValuators = _lib.XSetDeviceValuators
XSetDeviceValuators.restype = c_int
XSetDeviceValuators.argtypes = [POINTER(Display), POINTER(XDevice), POINTER(c_int), c_int, c_int]

# /usr/include/X11/extensions/XInput.h:5971
XGetDeviceControl = _lib.XGetDeviceControl
XGetDeviceControl.restype = POINTER(XDeviceControl)
XGetDeviceControl.argtypes = [POINTER(Display), POINTER(XDevice), c_int]

# /usr/include/X11/extensions/XInput.h:5977
XChangeDeviceControl = _lib.XChangeDeviceControl
XChangeDeviceControl.restype = c_int
XChangeDeviceControl.argtypes = [POINTER(Display), POINTER(XDevice), c_int, POINTER(XDeviceControl)]

# /usr/include/X11/extensions/XInput.h:5984
XSelectExtensionEvent = _lib.XSelectExtensionEvent
XSelectExtensionEvent.restype = c_int
XSelectExtensionEvent.argtypes = [POINTER(Display), Window, POINTER(XEventClass), c_int]

# /usr/include/X11/extensions/XInput.h:5991
XGetSelectedExtensionEvents = _lib.XGetSelectedExtensionEvents
XGetSelectedExtensionEvents.restype = c_int
XGetSelectedExtensionEvents.argtypes = [POINTER(Display), Window, POINTER(c_int), POINTER(POINTER(XEventClass)), POINTER(c_int), POINTER(POINTER(XEventClass))]

# /usr/include/X11/extensions/XInput.h:6000
XChangeDeviceDontPropagateList = _lib.XChangeDeviceDontPropagateList
XChangeDeviceDontPropagateList.restype = c_int
XChangeDeviceDontPropagateList.argtypes = [POINTER(Display), Window, c_int, POINTER(XEventClass), c_int]

# /usr/include/X11/extensions/XInput.h:6008
XGetDeviceDontPropagateList = _lib.XGetDeviceDontPropagateList
XGetDeviceDontPropagateList.restype = POINTER(XEventClass)
XGetDeviceDontPropagateList.argtypes = [POINTER(Display), Window, POINTER(c_int)]

XEvent = pyglet.libs.x11.xlib.XEvent
# /usr/include/X11/extensions/XInput.h:6014
XSendExtensionEvent = _lib.XSendExtensionEvent
XSendExtensionEvent.restype = c_int
XSendExtensionEvent.argtypes = [POINTER(Display), POINTER(XDevice), Window, c_int, c_int, POINTER(XEventClass), POINTER(XEvent)]

# /usr/include/X11/extensions/XInput.h:6024
XGetDeviceMotionEvents = _lib.XGetDeviceMotionEvents
XGetDeviceMotionEvents.restype = POINTER(XDeviceTimeCoord)
XGetDeviceMotionEvents.argtypes = [POINTER(Display), POINTER(XDevice), Time, Time, POINTER(c_int), POINTER(c_int), POINTER(c_int)]

# /usr/include/X11/extensions/XInput.h:6034
XFreeDeviceMotionEvents = _lib.XFreeDeviceMotionEvents
XFreeDeviceMotionEvents.restype = None
XFreeDeviceMotionEvents.argtypes = [POINTER(XDeviceTimeCoord)]

# /usr/include/X11/extensions/XInput.h:6038
XFreeDeviceControl = _lib.XFreeDeviceControl
XFreeDeviceControl.restype = None
XFreeDeviceControl.argtypes = [POINTER(XDeviceControl)]


__all__ = ['sz_xGetExtensionVersionReq', 'sz_xGetExtensionVersionReply',
'sz_xListInputDevicesReq', 'sz_xListInputDevicesReply', 'sz_xOpenDeviceReq',
'sz_xOpenDeviceReply', 'sz_xCloseDeviceReq', 'sz_xSetDeviceModeReq',
'sz_xSetDeviceModeReply', 'sz_xSelectExtensionEventReq',
'sz_xGetSelectedExtensionEventsReq', 'sz_xGetSelectedExtensionEventsReply',
'sz_xChangeDeviceDontPropagateListReq', 'sz_xGetDeviceDontPropagateListReq',
'sz_xGetDeviceDontPropagateListReply', 'sz_xGetDeviceMotionEventsReq',
'sz_xGetDeviceMotionEventsReply', 'sz_xChangeKeyboardDeviceReq',
'sz_xChangeKeyboardDeviceReply', 'sz_xChangePointerDeviceReq',
'sz_xChangePointerDeviceReply', 'sz_xGrabDeviceReq', 'sz_xGrabDeviceReply',
'sz_xUngrabDeviceReq', 'sz_xGrabDeviceKeyReq', 'sz_xGrabDeviceKeyReply',
'sz_xUngrabDeviceKeyReq', 'sz_xGrabDeviceButtonReq',
'sz_xGrabDeviceButtonReply', 'sz_xUngrabDeviceButtonReq',
'sz_xAllowDeviceEventsReq', 'sz_xGetDeviceFocusReq',
'sz_xGetDeviceFocusReply', 'sz_xSetDeviceFocusReq',
'sz_xGetFeedbackControlReq', 'sz_xGetFeedbackControlReply',
'sz_xChangeFeedbackControlReq', 'sz_xGetDeviceKeyMappingReq',
'sz_xGetDeviceKeyMappingReply', 'sz_xChangeDeviceKeyMappingReq',
'sz_xGetDeviceModifierMappingReq', 'sz_xSetDeviceModifierMappingReq',
'sz_xSetDeviceModifierMappingReply', 'sz_xGetDeviceButtonMappingReq',
'sz_xGetDeviceButtonMappingReply', 'sz_xSetDeviceButtonMappingReq',
'sz_xSetDeviceButtonMappingReply', 'sz_xQueryDeviceStateReq',
'sz_xQueryDeviceStateReply', 'sz_xSendExtensionEventReq', 'sz_xDeviceBellReq',
'sz_xSetDeviceValuatorsReq', 'sz_xSetDeviceValuatorsReply',
'sz_xGetDeviceControlReq', 'sz_xGetDeviceControlReply',
'sz_xChangeDeviceControlReq', 'sz_xChangeDeviceControlReply', 'Dont_Check',
'XInput_Initial_Release', 'XInput_Add_XDeviceBell',
'XInput_Add_XSetDeviceValuators', 'XInput_Add_XChangeDeviceControl',
'XInput_Add_DevicePresenceNotify', 'XI_Absent', 'XI_Present',
'XI_Initial_Release_Major', 'XI_Initial_Release_Minor',
'XI_Add_XDeviceBell_Major', 'XI_Add_XDeviceBell_Minor',
'XI_Add_XSetDeviceValuators_Major', 'XI_Add_XSetDeviceValuators_Minor',
'XI_Add_XChangeDeviceControl_Major', 'XI_Add_XChangeDeviceControl_Minor',
'XI_Add_DevicePresenceNotify_Major', 'XI_Add_DevicePresenceNotify_Minor',
'DEVICE_RESOLUTION', 'DEVICE_ABS_CALIB', 'DEVICE_CORE', 'DEVICE_ENABLE',
'DEVICE_ABS_AREA', 'NoSuchExtension', 'COUNT', 'CREATE', 'NewPointer',
'NewKeyboard', 'XPOINTER', 'XKEYBOARD', 'UseXKeyboard', 'IsXPointer',
'IsXKeyboard', 'IsXExtensionDevice', 'IsXExtensionKeyboard',
'IsXExtensionPointer', 'AsyncThisDevice', 'SyncThisDevice',
'ReplayThisDevice', 'AsyncOtherDevices', 'AsyncAll', 'SyncAll',
'FollowKeyboard', 'RevertToFollowKeyboard', 'DvAccelNum', 'DvAccelDenom',
'DvThreshold', 'DvKeyClickPercent', 'DvPercent', 'DvPitch', 'DvDuration',
'DvLed', 'DvLedMode', 'DvKey', 'DvAutoRepeatMode', 'DvString', 'DvInteger',
'DeviceMode', 'Relative', 'Absolute', 'ProximityState', 'InProximity',
'OutOfProximity', 'AddToList', 'DeleteFromList', 'KeyClass', 'ButtonClass',
'ValuatorClass', 'FeedbackClass', 'ProximityClass', 'FocusClass',
'OtherClass', 'KbdFeedbackClass', 'PtrFeedbackClass', 'StringFeedbackClass',
'IntegerFeedbackClass', 'LedFeedbackClass', 'BellFeedbackClass',
'_devicePointerMotionHint', '_deviceButton1Motion', '_deviceButton2Motion',
'_deviceButton3Motion', '_deviceButton4Motion', '_deviceButton5Motion',
'_deviceButtonMotion', '_deviceButtonGrab', '_deviceOwnerGrabButton',
'_noExtensionEvent', '_devicePresence', 'DeviceAdded', 'DeviceRemoved',
'DeviceEnabled', 'DeviceDisabled', 'DeviceUnrecoverable', 'XI_BadDevice',
'XI_BadEvent', 'XI_BadMode', 'XI_DeviceBusy', 'XI_BadClass', 'XEventClass',
'XExtensionVersion', '_deviceKeyPress', '_deviceKeyRelease',
'_deviceButtonPress', '_deviceButtonRelease', '_deviceMotionNotify',
'_deviceFocusIn', '_deviceFocusOut', '_proximityIn', '_proximityOut',
'_deviceStateNotify', '_deviceMappingNotify', '_changeDeviceNotify',
'XDeviceKeyEvent', 'XDeviceKeyPressedEvent', 'XDeviceKeyReleasedEvent',
'XDeviceButtonEvent', 'XDeviceButtonPressedEvent',
'XDeviceButtonReleasedEvent', 'XDeviceMotionEvent', 'XDeviceFocusChangeEvent',
'XDeviceFocusInEvent', 'XDeviceFocusOutEvent', 'XProximityNotifyEvent',
'XProximityInEvent', 'XProximityOutEvent', 'XInputClass',
'XDeviceStateNotifyEvent', 'XValuatorStatus', 'XKeyStatus', 'XButtonStatus',
'XDeviceMappingEvent', 'XChangeDeviceNotifyEvent',
'XDevicePresenceNotifyEvent', 'XFeedbackState', 'XKbdFeedbackState',
'XPtrFeedbackState', 'XIntegerFeedbackState', 'XStringFeedbackState',
'XBellFeedbackState', 'XLedFeedbackState', 'XFeedbackControl',
'XPtrFeedbackControl', 'XKbdFeedbackControl', 'XStringFeedbackControl',
'XIntegerFeedbackControl', 'XBellFeedbackControl', 'XLedFeedbackControl',
'XDeviceControl', 'XDeviceResolutionControl', 'XDeviceResolutionState',
'XDeviceAbsCalibControl', 'XDeviceAbsCalibState', 'XDeviceAbsAreaControl',
'XDeviceAbsAreaState', 'XDeviceCoreControl', 'XDeviceCoreState',
'XDeviceEnableControl', 'XDeviceEnableState', 'XAnyClassPtr', 'XAnyClassInfo',
'XDeviceInfoPtr', 'XDeviceInfo', 'XKeyInfoPtr', 'XKeyInfo', 'XButtonInfoPtr',
'XButtonInfo', 'XAxisInfoPtr', 'XAxisInfo', 'XValuatorInfoPtr',
'XValuatorInfo', 'XInputClassInfo', 'XDevice', 'XEventList',
'XDeviceTimeCoord', 'XDeviceState', 'XValuatorState', 'XKeyState',
'XButtonState', 'XChangeKeyboardDevice', 'XChangePointerDevice',
'XGrabDevice', 'XUngrabDevice', 'XGrabDeviceKey', 'XUngrabDeviceKey',
'XGrabDeviceButton', 'XUngrabDeviceButton', 'XAllowDeviceEvents',
'XGetDeviceFocus', 'XSetDeviceFocus', 'XGetFeedbackControl',
'XFreeFeedbackList', 'XChangeFeedbackControl', 'XDeviceBell',
'XGetDeviceKeyMapping', 'XChangeDeviceKeyMapping',
'XGetDeviceModifierMapping', 'XSetDeviceModifierMapping',
'XSetDeviceButtonMapping', 'XGetDeviceButtonMapping', 'XQueryDeviceState',
'XFreeDeviceState', 'XGetExtensionVersion', 'XListInputDevices',
'XFreeDeviceList', 'XOpenDevice', 'XCloseDevice', 'XSetDeviceMode',
'XSetDeviceValuators', 'XGetDeviceControl', 'XChangeDeviceControl',
'XSelectExtensionEvent', 'XGetSelectedExtensionEvents',
'XChangeDeviceDontPropagateList', 'XGetDeviceDontPropagateList',
'XSendExtensionEvent', 'XGetDeviceMotionEvents', 'XFreeDeviceMotionEvents',
'XFreeDeviceControl']
