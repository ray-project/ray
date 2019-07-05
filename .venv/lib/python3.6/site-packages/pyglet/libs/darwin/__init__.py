from __future__ import absolute_import
import pyglet

# Cocoa implementation:
if pyglet.options['darwin_cocoa']:

    from .cocoapy import *

# Carbon implementation:
else:
    from .types import *
    from .constants import *

    carbon = pyglet.lib.load_library(
        framework='/System/Library/Frameworks/Carbon.framework')

    # No 64-bit version of quicktime
    # (It was replaced with QTKit, which is written in Objective-C)
    quicktime = pyglet.lib.load_library(
        framework='/System/Library/Frameworks/QuickTime.framework')

    carbon.GetEventDispatcherTarget.restype = EventTargetRef
    carbon.ReceiveNextEvent.argtypes = \
        [c_uint32, c_void_p, c_double, c_ubyte, POINTER(EventRef)]
    #carbon.GetWindowPort.restype = agl.AGLDrawable
    EventHandlerProcPtr = CFUNCTYPE(c_int, c_int, c_void_p, c_void_p)

    # CarbonEvent functions are not available in 64-bit Carbon
    carbon.NewEventHandlerUPP.restype = c_void_p
    carbon.GetCurrentKeyModifiers = c_uint32
    carbon.NewRgn.restype = RgnHandle
    carbon.CGDisplayBounds.argtypes = [c_void_p]
    carbon.CGDisplayBounds.restype = CGRect

    def create_cfstring(text):
        return carbon.CFStringCreateWithCString(c_void_p(), 
                                                text.encode('utf8'),
                                                kCFStringEncodingUTF8)

    def _oscheck(result):
        if result != noErr:
            raise RuntimeError('Carbon error %d' % result)
        return result
