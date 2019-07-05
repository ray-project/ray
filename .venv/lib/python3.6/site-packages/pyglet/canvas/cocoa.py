# Note: The display mode API used here is Mac OS 10.6 only.

'''
'''
from __future__ import absolute_import

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *
from ctypes import util

from pyglet import app
from .base import Display, Screen, ScreenMode, Canvas

from pyglet.libs.darwin.cocoapy import *


class CocoaDisplay(Display):

    def get_screens(self):
        maxDisplays = 256 
        activeDisplays = (CGDirectDisplayID * maxDisplays)()
        count = c_uint32()
        quartz.CGGetActiveDisplayList(maxDisplays, activeDisplays, byref(count))
        return [CocoaScreen(self, displayID) for displayID in list(activeDisplays)[:count.value]]


class CocoaScreen(Screen):

    def __init__(self, display, displayID):
        bounds = quartz.CGDisplayBounds(displayID)
        # FIX ME:
        # Probably need to convert the origin coordinates depending on context:
        # http://www.cocoabuilder.com/archive/cocoa/233492-ns-cg-rect-conversion-and-screen-coordinates.html
        x, y = bounds.origin.x, bounds.origin.y
        width, height = bounds.size.width, bounds.size.height
        super(CocoaScreen, self).__init__(display, int(x), int(y), int(width), int(height))
        self._cg_display_id = displayID
        # Save the default mode so we can restore to it.
        self._default_mode = self.get_mode()

    # FIX ME:
    # This method is needed to get multi-monitor support working properly.
    # However the NSScreens.screens() message currently sends out a warning:
    # "*** -[NSLock unlock]: lock (<NSLock: 0x...> '(null)') unlocked when not locked"
    # on Snow Leopard and apparently causes python to crash on Lion.
    # 
    # def get_nsscreen(self):
    #     """Returns the NSScreen instance that matches our CGDirectDisplayID."""
    #     NSScreen = ObjCClass('NSScreen')
    #     # Get a list of all currently active NSScreens and then search through
    #     # them until we find one that matches our CGDisplayID.
    #     screen_array = NSScreen.screens()
    #     count = screen_array.count()
    #     for i in range(count):
    #         nsscreen = screen_array.objectAtIndex_(i)
    #         screenInfo = nsscreen.deviceDescription()
    #         displayID = screenInfo.objectForKey_(get_NSString('NSScreenNumber'))
    #         displayID = displayID.intValue()
    #         if displayID == self._cg_display_id:
    #             return nsscreen
    #     return None

    def get_matching_configs(self, template):
        canvas = CocoaCanvas(self.display, self, None)
        return template.match(canvas)

    def get_modes(self):
        cgmodes = c_void_p(quartz.CGDisplayCopyAllDisplayModes(self._cg_display_id, None))
        modes = [ CocoaScreenMode(self, cgmode) for cgmode in cfarray_to_list(cgmodes) ]
        cf.CFRelease(cgmodes)
        return modes

    def get_mode(self):
        cgmode = c_void_p(quartz.CGDisplayCopyDisplayMode(self._cg_display_id))
        mode = CocoaScreenMode(self, cgmode)
        quartz.CGDisplayModeRelease(cgmode)
        return mode

    def set_mode(self, mode): 
        assert mode.screen is self
        quartz.CGDisplayCapture(self._cg_display_id)
        quartz.CGDisplaySetDisplayMode(self._cg_display_id, mode.cgmode, None)
        self.width = mode.width
        self.height = mode.height

    def restore_mode(self):
        quartz.CGDisplaySetDisplayMode(self._cg_display_id, self._default_mode.cgmode, None)
        quartz.CGDisplayRelease(self._cg_display_id)

    def capture_display(self):
        quartz.CGDisplayCapture(self._cg_display_id)

    def release_display(self):
        quartz.CGDisplayRelease(self._cg_display_id)


class CocoaScreenMode(ScreenMode):

    def __init__(self, screen, cgmode):
        super(CocoaScreenMode, self).__init__(screen)
        quartz.CGDisplayModeRetain(cgmode)
        self.cgmode = cgmode
        self.width = int(quartz.CGDisplayModeGetWidth(cgmode))
        self.height = int(quartz.CGDisplayModeGetHeight(cgmode))
        self.depth = self.getBitsPerPixel(cgmode)
        self.rate = quartz.CGDisplayModeGetRefreshRate(cgmode)

    def __del__(self):
        quartz.CGDisplayModeRelease(self.cgmode)
        self.cgmode = None
        
    def getBitsPerPixel(self, cgmode):
        # from /System/Library/Frameworks/IOKit.framework/Headers/graphics/IOGraphicsTypes.h
        IO8BitIndexedPixels = "PPPPPPPP"
        IO16BitDirectPixels = "-RRRRRGGGGGBBBBB"
        IO32BitDirectPixels = "--------RRRRRRRRGGGGGGGGBBBBBBBB"

        cfstring = c_void_p(quartz.CGDisplayModeCopyPixelEncoding(cgmode))
        pixelEncoding = cfstring_to_string(cfstring)
        cf.CFRelease(cfstring)

        if pixelEncoding == IO8BitIndexedPixels: return 8
        if pixelEncoding == IO16BitDirectPixels: return 16
        if pixelEncoding == IO32BitDirectPixels: return 32
        return 0

                   
class CocoaCanvas(Canvas):

    def __init__(self, display, screen, nsview):
        super(CocoaCanvas, self).__init__(display)
        self.screen = screen
        self.nsview = nsview
