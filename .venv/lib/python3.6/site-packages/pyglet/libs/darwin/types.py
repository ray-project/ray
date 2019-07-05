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

'''
'''

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *

Boolean = c_ubyte           # actually an unsigned char
Fixed = c_int32
ItemCount = c_uint32
ByteOffset = ByteCount = c_uint32

class Rect(Structure):
    _fields_ = [
        ('top', c_short),
        ('left', c_short),
        ('bottom', c_short),
        ('right', c_short)
    ]

class Point(Structure):
    _fields_ = [
        ('v', c_short),
        ('h', c_short),
    ]

class CGPoint(Structure):
    _fields_ = [
        ('x', c_float),
        ('y', c_float),
    ]

class CGSize(Structure):
    _fields_ = [
        ('width', c_float),
        ('height', c_float)
    ]

class CGRect(Structure):
    _fields_ = [
        ('origin', CGPoint),
        ('size', CGSize)
    ]
    __slots__ = ['origin', 'size']

CGDirectDisplayID = c_void_p
CGDisplayCount = c_uint32
CGTableCount = c_uint32
CGDisplayCoord = c_int32
CGByteValue = c_ubyte
CGOpenGLDisplayMask = c_uint32
CGRefreshRate = c_double
CGCaptureOptions = c_uint32

HIPoint = CGPoint
HISize = CGSize
HIRect = CGRect

class EventTypeSpec(Structure):
    _fields_ = [
        ('eventClass', c_uint32),
        ('eventKind', c_uint32)
    ]

WindowRef = c_void_p
EventRef = c_void_p
EventTargetRef = c_void_p
EventHandlerRef = c_void_p

MenuRef = c_void_p
MenuID = c_int16
MenuItemIndex = c_uint16
MenuCommand = c_uint32

CFStringEncoding = c_uint
WindowClass = c_uint32
WindowAttributes = c_uint32
WindowPositionMethod = c_uint32
EventMouseButton = c_uint16
EventMouseWheelAxis = c_uint16

OSType = c_uint32
OSStatus = c_int32


class MouseTrackingRegionID(Structure):
    _fields_ = [('signature', OSType),
                ('id', c_int32)]

MouseTrackingRef = c_void_p

RgnHandle = c_void_p

class ProcessSerialNumber(Structure):
    _fields_ = [('highLongOfPSN', c_uint32),
                ('lowLongOfPSN', c_uint32)]


class HICommand_Menu(Structure):
    _fields_ = [
        ('menuRef', MenuRef),
        ('menuItemIndex', MenuItemIndex),
    ]

class HICommand(Structure):
    _fields_ = [
        ('attributes', c_uint32),
        ('commandID', c_uint32),
        ('menu', HICommand_Menu)
    ]

class EventRecord(Structure):
    _fields_ = [
        ('what', c_uint16),
        ('message', c_uint32),
        ('when', c_uint32),
        ('where', Point),
        ('modifiers', c_uint16)
    ]

class RGBColor(Structure):
    _fields_ = [
        ('red', c_ushort),
        ('green', c_ushort),
        ('blue', c_ushort)
    ]

class TabletProximityRec(Structure):
    _fields_ = (
        ('vendorID', c_uint16),
        ('tabletID', c_uint16),
        ('pointerID', c_uint16),
        ('deviceID', c_uint16),
        ('systemTabletID', c_uint16),
        ('vendorPointerType', c_uint16),
        ('pointerSerialNumber', c_uint32),
        ('uniqueID', c_uint64),
        ('capabilityMask', c_uint32),
        ('pointerType', c_uint8),
        ('enterProximity', c_uint8),
    )

class TabletPointRec(Structure):
    _fields_ = (
        ('absX', c_int32),
        ('absY', c_int32),
        ('absZ', c_int32),
        ('buttons', c_uint16),
        ('pressure', c_uint16),
        ('tiltX', c_int16),
        ('tiltY', c_int16),
        ('rotation', c_uint16),
        ('tangentialPressure', c_int16),
        ('deviceID', c_uint16),
        ('vendor1', c_int16),
        ('vendor2', c_int16),
        ('vendor3', c_int16),
    )
