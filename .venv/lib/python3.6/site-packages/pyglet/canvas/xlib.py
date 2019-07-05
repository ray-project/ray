#!/usr/bin/env python

'''
'''
from __future__ import print_function
from __future__ import absolute_import
from builtins import range

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *
import ctypes

from pyglet import app
from pyglet.app.xlib import XlibSelectDevice
from .base import Display, Screen, ScreenMode, Canvas

from . import xlib_vidmoderestore

# XXX
#from pyglet.window import NoSuchDisplayException
class NoSuchDisplayException(Exception):
    pass

from pyglet.libs.x11 import xlib
try:
    from pyglet.libs.x11 import xinerama
    _have_xinerama = True
except:
    _have_xinerama = False

try:
    from pyglet.libs.x11 import xsync
    _have_xsync = True
except:
    _have_xsync = False

try:
    from pyglet.libs.x11 import xf86vmode
    _have_xf86vmode = True
except:
    _have_xf86vmode = False

# Set up error handler
def _error_handler(display, event):
    # By default, all errors are silently ignored: this has a better chance
    # of working than the default behaviour of quitting ;-)
    #
    # We've actually never seen an error that was our fault; they're always
    # driver bugs (and so the reports are useless).  Nevertheless, set
    # environment variable PYGLET_DEBUG_X11 to 1 to get dumps of the error
    # and a traceback (execution will continue).
    import pyglet
    if pyglet.options['debug_x11']:
        event = event.contents
        buf = c_buffer(1024)
        xlib.XGetErrorText(display, event.error_code, buf, len(buf))
        print('X11 error:', buf.value)
        print('   serial:', event.serial)
        print('  request:', event.request_code)
        print('    minor:', event.minor_code)
        print(' resource:', event.resourceid)

        import traceback
        print('Python stack trace (innermost last):')
        traceback.print_stack()
    return 0
_error_handler_ptr = xlib.XErrorHandler(_error_handler)
xlib.XSetErrorHandler(_error_handler_ptr)

class XlibDisplay(XlibSelectDevice, Display):
    _display = None     # POINTER(xlib.Display)

    _x_im = None        # X input method
                        # TODO close _x_im when display connection closed.
    _enable_xsync = False
    _screens = None     # Cache of get_screens()

    def __init__(self, name=None, x_screen=None):
        if x_screen is None:
            x_screen = 0

        self._display = xlib.XOpenDisplay(name)
        if not self._display:
            raise NoSuchDisplayException('Cannot connect to "%s"' % name)

        screen_count = xlib.XScreenCount(self._display)
        if x_screen >= screen_count:
            raise NoSuchDisplayException(
                'Display "%s" has no screen %d' % (name, x_screen))

        super(XlibDisplay, self).__init__()
        self.name = name
        self.x_screen = x_screen

        self._fileno = xlib.XConnectionNumber(self._display)
        self._window_map = {}

        # Initialise XSync
        if _have_xsync:
            event_base = c_int()
            error_base = c_int()
            if xsync.XSyncQueryExtension(self._display, 
                                         byref(event_base),
                                         byref(error_base)):
                major_version = c_int()
                minor_version = c_int()
                if xsync.XSyncInitialize(self._display,
                                         byref(major_version),
                                         byref(minor_version)):
                    self._enable_xsync = True

        # Add to event loop select list.  Assume we never go away.
        app.platform_event_loop._select_devices.add(self)

    def get_screens(self):
        if self._screens:
            return self._screens

        if _have_xinerama and xinerama.XineramaIsActive(self._display):
            number = c_int()
            infos = xinerama.XineramaQueryScreens(self._display, 
                                                  byref(number))
            infos = cast(infos, 
                 POINTER(xinerama.XineramaScreenInfo * number.value)).contents
            self._screens = []
            using_xinerama = number.value > 1
            for info in infos:
                self._screens.append(XlibScreen(self,
                                                info.x_org,
                                                info.y_org,
                                                info.width,
                                                info.height,
                                                using_xinerama))
            xlib.XFree(infos)
        else:
            # No xinerama
            screen_info = xlib.XScreenOfDisplay(self._display, self.x_screen)
            screen = XlibScreen(self,
                                0, 0,
                                screen_info.contents.width,
                                screen_info.contents.height,
                                False)
            self._screens = [screen]
        return self._screens

    # XlibSelectDevice interface

    def fileno(self):
        return self._fileno

    def select(self):
        e = xlib.XEvent()
        while xlib.XPending(self._display):
            xlib.XNextEvent(self._display, e)

            # Key events are filtered by the xlib window event
            # handler so they get a shot at the prefiltered event.
            if e.xany.type not in (xlib.KeyPress, xlib.KeyRelease):
                if xlib.XFilterEvent(e, e.xany.window):
                    continue
            try:
                dispatch = self._window_map[e.xany.window]
            except KeyError:
                continue

            dispatch(e)

    def poll(self):
        return xlib.XPending(self._display)

class XlibScreen(Screen):
    _initial_mode = None

    def __init__(self, display, x, y, width, height, xinerama):
        super(XlibScreen, self).__init__(display, x, y, width, height)
        self._xinerama = xinerama
 
    def get_matching_configs(self, template):
        canvas = XlibCanvas(self.display, None)
        configs = template.match(canvas)
        # XXX deprecate
        for config in configs:
            config.screen = self
        return configs

    def get_modes(self):
        if not _have_xf86vmode:
            return []

        if self._xinerama:
            # If Xinerama/TwinView is enabled, xf86vidmode's modelines
            # correspond to metamodes, which don't distinguish one screen from
            # another.  XRandR (broken) or NV (complicated) extensions needed.
            return []

        count = ctypes.c_int()
        info_array = \
            ctypes.POINTER(ctypes.POINTER(xf86vmode.XF86VidModeModeInfo))()
        xf86vmode.XF86VidModeGetAllModeLines(
            self.display._display, self.display.x_screen, count, info_array)

        # Copy modes out of list and free list
        modes = []
        for i in range(count.value):
            info = xf86vmode.XF86VidModeModeInfo()
            ctypes.memmove(ctypes.byref(info), 
                           ctypes.byref(info_array.contents[i]), 
                           ctypes.sizeof(info))
            modes.append(XlibScreenMode(self, info))
            if info.privsize:
                xlib.XFree(info.private)
        xlib.XFree(info_array)

        return modes

    def get_mode(self):
        modes = self.get_modes()
        if modes:
            return modes[0]
        return None

    def set_mode(self, mode):
        assert mode.screen is self

        if not self._initial_mode:
            self._initial_mode = self.get_mode()
            xlib_vidmoderestore.set_initial_mode(self._initial_mode)

        xf86vmode.XF86VidModeSwitchToMode(self.display._display, 
            self.display.x_screen, mode.info)
        xlib.XFlush(self.display._display)
        xf86vmode.XF86VidModeSetViewPort(self.display._display,
            self.display.x_screen, 0, 0)
        xlib.XFlush(self.display._display)

        self.width = mode.width
        self.height = mode.height

    def restore_mode(self):
        if self._initial_mode:
            self.set_mode(self._initial_mode)

    def __repr__(self):
        return 'XlibScreen(display=%r, x=%d, y=%d, ' \
               'width=%d, height=%d, xinerama=%d)' % \
            (self.display, self.x, self.y, self.width, self.height,
             self._xinerama)

class XlibScreenMode(ScreenMode):
    def __init__(self, screen, info):
        super(XlibScreenMode, self).__init__(screen)
        self.info = info
        self.width = info.hdisplay
        self.height = info.vdisplay
        self.rate = info.dotclock
        self.depth = None

class XlibCanvas(Canvas):
    def __init__(self, display, x_window):
        super(XlibCanvas, self).__init__(display)
        self.x_window = x_window
