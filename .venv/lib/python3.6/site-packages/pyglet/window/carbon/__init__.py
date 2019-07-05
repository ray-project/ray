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
from __future__ import division
from builtins import str

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *
import os.path
import unicodedata
import warnings

import pyglet
from pyglet.window import WindowException, \
    BaseWindow, MouseCursor, DefaultMouseCursor, _PlatformEventHandler
from pyglet.window import key
from pyglet.window import mouse
from pyglet.window import event
from pyglet.canvas.carbon import CarbonCanvas

from pyglet.libs.darwin import *
from pyglet.libs.darwin import _oscheck
from pyglet.libs.darwin.quartzkey import keymap, charmap

from pyglet.event import EventDispatcher

# Map symbol,modifiers -> motion
# Determined by experiment with TextEdit.app
_motion_map = {
    (key.UP, False):                    key.MOTION_UP,
    (key.RIGHT, False):                 key.MOTION_RIGHT,
    (key.DOWN, False):                  key.MOTION_DOWN,
    (key.LEFT, False):                  key.MOTION_LEFT,
    (key.LEFT, key.MOD_OPTION):         key.MOTION_PREVIOUS_WORD,
    (key.RIGHT, key.MOD_OPTION):        key.MOTION_NEXT_WORD,
    (key.LEFT, key.MOD_COMMAND):        key.MOTION_BEGINNING_OF_LINE,
    (key.RIGHT, key.MOD_COMMAND):       key.MOTION_END_OF_LINE,
    (key.PAGEUP, False):                key.MOTION_PREVIOUS_PAGE,
    (key.PAGEDOWN, False):              key.MOTION_NEXT_PAGE,
    (key.HOME, False):                  key.MOTION_BEGINNING_OF_FILE,
    (key.END, False):                   key.MOTION_END_OF_FILE,
    (key.UP, key.MOD_COMMAND):          key.MOTION_BEGINNING_OF_FILE,
    (key.DOWN, key.MOD_COMMAND):        key.MOTION_END_OF_FILE,
    (key.BACKSPACE, False):             key.MOTION_BACKSPACE,
    (key.DELETE, False):                key.MOTION_DELETE,
}


class CarbonMouseCursor(MouseCursor):
    drawable = False
    def __init__(self, theme):
        self.theme = theme

def CarbonEventHandler(event_class, event_kind):
    return _PlatformEventHandler((event_class, event_kind))

class CarbonWindow(BaseWindow):
    _window = None                  # Carbon WindowRef

    # Window properties
    _minimum_size = None
    _maximum_size = None
    _event_dispatcher = None
    _current_modifiers = 0
    _mapped_modifers = 0
    _carbon_event_handlers = []
    _carbon_event_handler_refs = []
    _track_ref = 0
    _track_region = None

    _mouse_exclusive = False
    _mouse_platform_visible = True
    _mouse_ignore_motion = False
    _mouse_button_state = 0

    def _recreate(self, changes):
        # We can't destroy the window while event handlers are active,
        # otherwise the (OS X) event dispatcher gets lost and segfaults.
        #
        # Defer actual recreation until dispatch_events next finishes.
        from pyglet import app
        app.platform_event_loop.post_event(self, 
                                           'on_recreate_immediate', changes)

    def on_recreate_immediate(self, changes):
        # The actual _recreate function.
        if ('context' in changes):
            self.context.detach()

        self._create()

    def _create(self):
        if self._window:
            # The window is about to be recreated; destroy everything
            # associated with the old window, then the window itself.
            self._remove_track_region()
            self._remove_event_handlers()
            self.context.detach()
            self.canvas = None
            carbon.DisposeWindow(self._window)
            self._window = None

        self._window = WindowRef()

        if self._fullscreen:
            rect = Rect()
            rect.left = 0
            rect.top = 0
            rect.right = self.screen.width
            rect.bottom = self.screen.height
            r = carbon.CreateNewWindow(kSimpleWindowClass,
                                       kWindowNoAttributes,
                                       byref(rect),
                                       byref(self._window))
            _oscheck(r)

            # Set window level to shield level
            level = carbon.CGShieldingWindowLevel()
            WindowGroupRef = c_void_p
            group = WindowGroupRef()
            _oscheck(carbon.CreateWindowGroup(0, byref(group)))
            _oscheck(carbon.SetWindowGroup(self._window, group))
            _oscheck(carbon.SetWindowGroupLevel(group, level))

            # Set black background
            color = RGBColor(0, 0, 0)
            _oscheck(carbon.SetWindowContentColor(self._window, byref(color)))

            self._mouse_in_window = True
            self.dispatch_event('on_resize', self._width, self._height)
            self.dispatch_event('on_show')
            self.dispatch_event('on_expose')
            self._view_x = (self.screen.width - self._width) // 2
            self._view_y = (self.screen.height - self._height) // 2
            self.canvas = CarbonCanvas(self.display, self.screen,
                                       carbon.GetWindowPort(self._window))
            self.canvas.bounds = (self._view_x, self._view_y, 
                                  self._width, self._height)
        else:
            # Create floating window
            rect = Rect()
            location = None # TODO
            if location is not None:
                rect.left = location[0]
                rect.top = location[1]
            else:
                rect.top = rect.left = 0
            rect.right = rect.left + self._width
            rect.bottom = rect.top + self._height

            styles = {
                self.WINDOW_STYLE_DEFAULT:  (kDocumentWindowClass,
                                             kWindowCloseBoxAttribute |
                                             kWindowCollapseBoxAttribute),
                self.WINDOW_STYLE_DIALOG:   (kDocumentWindowClass,
                                             kWindowCloseBoxAttribute),
                self.WINDOW_STYLE_TOOL:     (kUtilityWindowClass,
                                             kWindowCloseBoxAttribute),
                self.WINDOW_STYLE_BORDERLESS:    (kSimpleWindowClass,
                                                  kWindowNoAttributes)
            }
            window_class, window_attributes = \
                styles.get(self._style, kDocumentWindowClass)

            if self._resizable:
                window_attributes |= (kWindowFullZoomAttribute |
                                      kWindowLiveResizeAttribute |
                                      kWindowResizableAttribute)

            r = carbon.CreateNewWindow(window_class,
                                       window_attributes,
                                       byref(rect),
                                       byref(self._window))
            _oscheck(r)

            if location is None:
                carbon.RepositionWindow(self._window, c_void_p(),
                    kWindowCascadeOnMainScreen)

            self.canvas = CarbonCanvas(self.display, self.screen,
                                       carbon.GetWindowPort(self._window))
            self._view_x = self._view_y = 0
                
        self.context.attach(self.canvas)

        self.set_caption(self._caption)

        # Get initial state
        self._event_dispatcher = carbon.GetEventDispatcherTarget()
        self._current_modifiers = carbon.GetCurrentKeyModifiers().value
        self._mapped_modifiers = self._map_modifiers(self._current_modifiers)

        # (re)install Carbon event handlers 
        self._install_event_handlers()

        self._create_track_region()

        self.switch_to() # XXX
        self.set_vsync(self._vsync)

        if self._visible:
            self.set_visible(True)

    def _create_track_region(self):
        self._remove_track_region()

        # Create a tracking region for the content part of the window
        # to receive enter/leave events.
        track_id = MouseTrackingRegionID()
        track_id.signature = DEFAULT_CREATOR_CODE
        track_id.id = 1
        self._track_ref = MouseTrackingRef()
        self._track_region = carbon.NewRgn()
        if self._fullscreen:
            carbon.SetRectRgn(self._track_region, 
                self._view_x, self._view_y, 
                self._view_x + self._width, self._view_y + self._height)
            options = kMouseTrackingOptionsGlobalClip
        else:
            carbon.GetWindowRegion(self._window, 
                kWindowContentRgn, self._track_region)
            options = kMouseTrackingOptionsGlobalClip
        carbon.CreateMouseTrackingRegion(self._window,  
            self._track_region, None, options,
            track_id, None, None,
            byref(self._track_ref))

    def _remove_track_region(self):
        if self._track_region:
            carbon.ReleaseMouseTrackingRegion(self._track_region)
            self._track_region = None

    def close(self):
        super(CarbonWindow, self).close()
        self._remove_event_handlers()
        self._remove_track_region()

        # Restore cursor visibility
        self.set_mouse_platform_visible(True)
        self.set_exclusive_mouse(False)

        if self._window:
            carbon.DisposeWindow(self._window)
        self._window = None

    def switch_to(self):
        self.context.set_current()
        '''
        agl.aglSetCurrentContext(self._agl_context)
        self._context.set_current()
        _aglcheck()
        # XXX TODO transpose gl[u]_info to gl.Context.attach
        gl_info.set_active_context()
        glu_info.set_active_context()
        '''

    def flip(self):
        self.draw_mouse_cursor()
        if self.context:
            self.context.flip()

    def _get_vsync(self):
        if self.context:
            return self.context.get_vsync()
        return self._vsync
    vsync = property(_get_vsync) # overrides BaseWindow property

    def set_vsync(self, vsync):
        if pyglet.options['vsync'] is not None:
            vsync = pyglet.options['vsync']
        self._vsync = vsync # _recreate depends on this
        if self.context:
            self.context.set_vsync(vsync)

    def dispatch_events(self):
        from pyglet import app
        app.platform_event_loop.dispatch_posted_events()

        self._allow_dispatch_event = True
        while self._event_queue:
            EventDispatcher.dispatch_event(self, *self._event_queue.pop(0))

        e = EventRef()
        result = carbon.ReceiveNextEvent(0, c_void_p(), 0, True, byref(e))
        while result == noErr:
            carbon.SendEventToEventTarget(e, self._event_dispatcher)
            carbon.ReleaseEvent(e)

            result = carbon.ReceiveNextEvent(0, c_void_p(), 0, True, byref(e))

        self._allow_dispatch_event = False

        # Return value from ReceiveNextEvent can be ignored if not
        # noErr; we check here only to look for new bugs.
        # eventLoopQuitErr: the inner event loop was quit, see
        # http://lists.apple.com/archives/Carbon-dev/2006/Jun/msg00850.html
        # Can occur when mixing with other toolkits, e.g. Tk.
        # Fixes issue 180.
        if result not in (eventLoopTimedOutErr, eventLoopQuitErr):
            raise WindowException('Error %d' % result)

    def dispatch_pending_events(self):
        while self._event_queue:
            EventDispatcher.dispatch_event(self, *self._event_queue.pop(0))

    def set_caption(self, caption):
        self._caption = caption
        s = create_cfstring(caption)
        carbon.SetWindowTitleWithCFString(self._window, s)
        carbon.CFRelease(s)

    def set_location(self, x, y):
        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        rect.right += x - rect.left
        rect.bottom += y - rect.top
        rect.left = x
        rect.top = y
        carbon.SetWindowBounds(self._window, kWindowContentRgn, byref(rect))

    def get_location(self):
        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        return rect.left, rect.top

    def set_size(self, width, height):
        if self._fullscreen:
            raise WindowException('Cannot set size of fullscreen window.')
        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        rect.right = rect.left + width
        rect.bottom = rect.top + height
        carbon.SetWindowBounds(self._window, kWindowContentRgn, byref(rect))

        self._width = width
        self._height = height
        self.dispatch_event('on_resize', width, height)
        self.dispatch_event('on_expose')

    def get_size(self):
        if self._fullscreen:
            return self._width, self._height
        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        return rect.right - rect.left, rect.bottom - rect.top

    def set_minimum_size(self, width, height):
        self._minimum_size = (width, height)
        minimum = HISize()
        minimum.width = width
        minimum.height = height
        if self._maximum_size:
            maximum = HISize()
            maximum.width, maximum.height = self._maximum_size
            maximum = byref(maximum)
        else:
            maximum = None
        carbon.SetWindowResizeLimits(self._window, 
            byref(minimum), maximum)

    def set_maximum_size(self, width, height):
        self._maximum_size = (width, height)
        maximum = HISize()
        maximum.width = width
        maximum.height = height
        if self._minimum_size:
            minimum = HISize()
            minimum.width, minimum.height = self._minimum_size
            minimum = byref(minimum)
        else:
            minimum = None
        carbon.SetWindowResizeLimits(self._window, 
            minimum, byref(maximum))

    def activate(self):
        carbon.ActivateWindow(self._window, 1)

        # Also make the application the "front" application.  TODO
        # maybe don't bring forward all of the application's windows?
        psn = ProcessSerialNumber()
        psn.highLongOfPSN = 0
        psn.lowLongOfPSN = kCurrentProcess
        carbon.SetFrontProcess(byref(psn))

    def set_visible(self, visible=True):
        self._visible = visible
        if visible:
            self.dispatch_event('on_resize', self._width, self._height)
            self.dispatch_event('on_show')
            self.dispatch_event('on_expose')
            carbon.ShowWindow(self._window)
        else:
            carbon.HideWindow(self._window)

    def minimize(self):
        self._mouse_in_window = False
        self.set_mouse_platform_visible()
        carbon.CollapseWindow(self._window, True)

    def maximize(self):
        # Maximum "safe" value, gets trimmed to screen size automatically.
        p = Point()
        p.v, p.h = 16000,16000 
        if not carbon.IsWindowInStandardState(self._window, byref(p), None):
            carbon.ZoomWindowIdeal(self._window, inZoomOut, byref(p))

    def set_mouse_platform_visible(self, platform_visible=None):
        if platform_visible is None:
            platform_visible = self._mouse_visible and \
                               not self._mouse_exclusive and \
                               not self._mouse_cursor.drawable
        if not self._mouse_in_window:
            platform_visible = True

        if self._mouse_in_window and \
           isinstance(self._mouse_cursor, CarbonMouseCursor):
            carbon.SetThemeCursor(self._mouse_cursor.theme)
        else:
            carbon.SetThemeCursor(kThemeArrowCursor)

        if self._mouse_platform_visible == platform_visible:
            return

        if platform_visible:
            carbon.ShowCursor()
        else:
            carbon.HideCursor()
        self._mouse_platform_visible = platform_visible

    def set_exclusive_mouse(self, exclusive=True):
        self._mouse_exclusive = exclusive
        if exclusive:
            # Move mouse to center of window
            rect = Rect()
            carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
            x = (rect.right + rect.left) / 2
            y = (rect.bottom + rect.top) / 2
            # Skip the next motion event, which would return a large delta.
            self._mouse_ignore_motion = True
            self.set_mouse_position(x, y, absolute=True)
            carbon.CGAssociateMouseAndMouseCursorPosition(False)
        else:
            carbon.CGAssociateMouseAndMouseCursorPosition(True)
        self.set_mouse_platform_visible()

    def set_mouse_position(self, x, y, absolute=False):
        point = CGPoint()
        if absolute:
            point.x = x
            point.y = y
        else:
            rect = Rect()
            carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
            point.x = x + rect.left
            point.y = rect.top + (rect.bottom - rect.top) - y
        carbon.CGWarpMouseCursorPosition(point)

    def set_exclusive_keyboard(self, exclusive=True):
        if exclusive:
            # Note: power switch can also be disabled, with
            # kUIOptionDisableSessionTerminate.  That seems
            # a little extreme though.
            carbon.SetSystemUIMode(kUIModeAllHidden,
                (kUIOptionDisableAppleMenu |
                 kUIOptionDisableProcessSwitch |
                 kUIOptionDisableForceQuit |
                 kUIOptionDisableHide))
        else:
            carbon.SetSystemUIMode(kUIModeNormal, 0)

    def get_system_mouse_cursor(self, name):
        if name == self.CURSOR_DEFAULT:
            return DefaultMouseCursor()

        themes = {
            self.CURSOR_CROSSHAIR:       kThemeCrossCursor,
            self.CURSOR_HAND:            kThemePointingHandCursor,
            self.CURSOR_HELP:            kThemeArrowCursor,
            self.CURSOR_NO:              kThemeNotAllowedCursor,
            self.CURSOR_SIZE:            kThemeArrowCursor,
            self.CURSOR_SIZE_UP:         kThemeResizeUpCursor,
            self.CURSOR_SIZE_UP_RIGHT:   kThemeArrowCursor,
            self.CURSOR_SIZE_RIGHT:      kThemeResizeRightCursor,
            self.CURSOR_SIZE_DOWN_RIGHT: kThemeArrowCursor,
            self.CURSOR_SIZE_DOWN:       kThemeResizeDownCursor,
            self.CURSOR_SIZE_DOWN_LEFT:  kThemeArrowCursor,
            self.CURSOR_SIZE_LEFT:       kThemeResizeLeftCursor,
            self.CURSOR_SIZE_UP_LEFT:    kThemeArrowCursor,
            self.CURSOR_SIZE_UP_DOWN:    kThemeResizeUpDownCursor,
            self.CURSOR_SIZE_LEFT_RIGHT: kThemeResizeLeftRightCursor,
            self.CURSOR_TEXT:            kThemeIBeamCursor,
            self.CURSOR_WAIT:            kThemeWatchCursor,
            self.CURSOR_WAIT_ARROW:      kThemeWatchCursor,
        }
        if name not in themes:
            raise RuntimeError('Unknown cursor name "%s"' % name)
        return CarbonMouseCursor(themes[name])

    def set_icon(self, *images):
        # Only use the biggest image
        image = images[0]
        size = image.width * image.height
        for img in images:
            if img.width * img.height > size:
                size = img.width * img.height
                image = img

        image = image.get_image_data()
        format = 'ARGB'
        pitch = -len(format) * image.width

        data = image.get_data(format, pitch)
        provider = carbon.CGDataProviderCreateWithData(
            None, data, len(data), None)

        colorspace = carbon.CGColorSpaceCreateDeviceRGB()

        cgi = carbon.CGImageCreate(
            image.width, image.height, 8, 32, -pitch,
            colorspace,
            kCGImageAlphaFirst,
            provider,
            None,
            True,
            kCGRenderingIntentDefault)

        carbon.SetApplicationDockTileImage(cgi)

        carbon.CGDataProviderRelease(provider)
        carbon.CGColorSpaceRelease(colorspace)

    # Non-public utilities

    def _update_drawable(self):
        if self.context:
            self.context.update_geometry()

        # Need a redraw
        self.dispatch_event('on_expose')

    def _update_track_region(self):
        if not self._fullscreen:
            carbon.GetWindowRegion(self._window, 
                kWindowContentRgn, self._track_region)
            carbon.ChangeMouseTrackingRegion(self._track_ref,
                self._track_region, None)

    def _install_event_handlers(self):
        self._remove_event_handlers()

        if self._fullscreen:
            target = carbon.GetApplicationEventTarget()
        else:
            target = carbon.GetWindowEventTarget(self._window)
        carbon.InstallStandardEventHandler(target)

        self._carbon_event_handlers = []
        self._carbon_event_handler_refs = []

        for func_name in self._platform_event_names:
            if not hasattr(self, func_name):
                continue

            func = getattr(self, func_name)
            self._install_event_handler(func)

    def _install_event_handler(self, func):
        if self._fullscreen:
            target = carbon.GetApplicationEventTarget()
        else:
            target = carbon.GetWindowEventTarget(self._window)

        for event_class, event_kind in func._platform_event_data:
            # TODO: could just build up array of class/kind
            proc = EventHandlerProcPtr(func)
            self._carbon_event_handlers.append(proc)
            upp = carbon.NewEventHandlerUPP(proc)
            types = EventTypeSpec()
            types.eventClass = event_class
            types.eventKind = event_kind
            handler_ref = EventHandlerRef()
            carbon.InstallEventHandler(
                target,
                upp,
                1,
                byref(types),
                c_void_p(),
                byref(handler_ref))
            self._carbon_event_handler_refs.append(handler_ref) 

    def _remove_event_handlers(self):
        for ref in self._carbon_event_handler_refs:
            carbon.RemoveEventHandler(ref)
        self._carbon_event_handler_refs = []
        self._carbon_event_handlers = []

    # Carbon event handlers

    @CarbonEventHandler(kEventClassTextInput, kEventTextInputUnicodeForKeyEvent)
    def _on_text_input(self, next_handler, ev, data):
        size = c_uint32()
        carbon.GetEventParameter(ev, kEventParamTextInputSendText,
            typeUTF8Text, c_void_p(), 0, byref(size), c_void_p())
        text = create_string_buffer(size.value)
        carbon.GetEventParameter(ev, kEventParamTextInputSendText,
            typeUTF8Text, c_void_p(), size.value, c_void_p(), byref(text))
        text = text.value.decode('utf8')

        raw_event = EventRef()
        carbon.GetEventParameter(ev, kEventParamTextInputSendKeyboardEvent,
            typeEventRef, c_void_p(), sizeof(raw_event), c_void_p(),
            byref(raw_event))
        symbol, modifiers = self._get_symbol_and_modifiers(raw_event)

        motion_modifiers = modifiers & \
            (key.MOD_COMMAND | key.MOD_CTRL | key.MOD_OPTION)
        if (symbol, motion_modifiers) in _motion_map:
            motion = _motion_map[symbol, motion_modifiers]
            if modifiers & key.MOD_SHIFT:
                self.dispatch_event('on_text_motion_select', motion)
            else:
                self.dispatch_event('on_text_motion', motion)
        elif ((unicodedata.category(text[0]) != 'Cc' or text == u'\r') and
            not (modifiers & key.MOD_COMMAND)):
            self.dispatch_event('on_text', text)
        return noErr

    @CarbonEventHandler(kEventClassKeyboard, kEventRawKeyUp)
    def _on_key_up(self, next_handler, ev, data):
        symbol, modifiers = self._get_symbol_and_modifiers(ev)
        if symbol:
            self.dispatch_event('on_key_release', symbol, modifiers)
        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassKeyboard, kEventRawKeyDown)
    def _on_key_down(self, next_handler, ev, data):
        symbol, modifiers = self._get_symbol_and_modifiers(ev)
        if symbol:
            self.dispatch_event('on_key_press', symbol, modifiers)
        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @staticmethod
    def _get_symbol_and_modifiers(ev):
        # The unicode char help processing virtual keycodes (see issue 405)
        wchar = c_wchar()
        carbon.GetEventParameter(ev, kEventParamKeyUnicodes,
            typeUnicodeText, c_void_p(), sizeof(wchar), c_void_p(), byref(wchar))
        try:
            wchar = str((wchar.value)).upper()
        except UnicodeEncodeError:
            # (this fix for issue 405 caused a bug itself (see comments 6-7);
            #  this try/except fixes it)
            wchar = None
        # If the unicode char is within charmap keys (ascii value), then we use
        # the corresponding symbol.
        if wchar in charmap.keys():
            symbol = charmap[wchar]
        else:
            sym = c_uint32()
            carbon.GetEventParameter(ev, kEventParamKeyCode,
                                     typeUInt32, c_void_p(), sizeof(sym), c_void_p(), byref(sym))
            symbol = keymap.get(sym.value, None)
            if symbol is None:
                symbol = key.user_key(sym.value)
        modifiers = c_uint32()
        carbon.GetEventParameter(ev, kEventParamKeyModifiers,
            typeUInt32, c_void_p(), sizeof(modifiers), c_void_p(),
            byref(modifiers))
        return (symbol, CarbonWindow._map_modifiers(modifiers.value))

    @staticmethod
    def _map_modifiers(modifiers):
        mapped_modifiers = 0
        if modifiers & (shiftKey | rightShiftKey):
            mapped_modifiers |= key.MOD_SHIFT
        if modifiers & (controlKey | rightControlKey):
            mapped_modifiers |= key.MOD_CTRL
        if modifiers & (optionKey | rightOptionKey):
            mapped_modifiers |= key.MOD_OPTION
        if modifiers & alphaLock:
            mapped_modifiers |= key.MOD_CAPSLOCK
        if modifiers & cmdKey:
            mapped_modifiers |= key.MOD_COMMAND

        return mapped_modifiers

    @CarbonEventHandler(kEventClassKeyboard, kEventRawKeyModifiersChanged)
    def _on_modifiers_changed(self, next_handler, ev, data):
        modifiers = c_uint32()
        carbon.GetEventParameter(ev, kEventParamKeyModifiers,
            typeUInt32, c_void_p(), sizeof(modifiers), c_void_p(),
            byref(modifiers))
        modifiers = modifiers.value
        deltas = modifiers ^ self._current_modifiers
        for mask, k in [
            (controlKey, key.LCTRL),
            (shiftKey, key.LSHIFT),
            (cmdKey, key.LCOMMAND),
            (optionKey, key.LOPTION),
            (rightShiftKey, key.RSHIFT),
            (rightOptionKey, key.ROPTION),
            (rightControlKey, key.RCTRL),
            (alphaLock, key.CAPSLOCK),
            (numLock, key.NUMLOCK)]:
            if deltas & mask:
                if modifiers & mask:
                    self.dispatch_event('on_key_press', 
                        k, self._mapped_modifiers)
                else:
                    self.dispatch_event('on_key_release',
                        k, self._mapped_modifiers)
        carbon.CallNextEventHandler(next_handler, ev)

        self._mapped_modifiers = self._map_modifiers(modifiers)
        self._current_modifiers = modifiers
        return noErr

    def _get_mouse_position(self, ev):
        position = HIPoint()
        carbon.GetEventParameter(ev, kEventParamMouseLocation,
            typeHIPoint, c_void_p(), sizeof(position), c_void_p(),
            byref(position))

        bounds = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(bounds))
        return (int(position.x - bounds.left - self._view_x), 
                int(position.y - bounds.top - self._view_y))

    def _get_mouse_buttons_changed(self):
        button_state = self._get_mouse_buttons()
        change = self._mouse_button_state ^ button_state
        self._mouse_button_state = button_state
        return change

    @staticmethod
    def _get_mouse_buttons():
        buttons = carbon.GetCurrentEventButtonState()
        button_state = 0
        if buttons & 0x1: button_state |= mouse.LEFT
        if buttons & 0x2: button_state |= mouse.RIGHT
        if buttons & 0x4: button_state |= mouse.MIDDLE
        return button_state
    
    @staticmethod
    def _get_modifiers(ev):
        modifiers = c_uint32()
        carbon.GetEventParameter(ev, kEventParamKeyModifiers,
            typeUInt32, c_void_p(), sizeof(modifiers), c_void_p(),
            byref(modifiers))

        return CarbonWindow._map_modifiers(modifiers.value)

    def _get_mouse_in_content(self, ev, x, y):
        if self._fullscreen:
            return 0 <= x < self._width and 0 <= y < self._height
        else:
            position = Point()
            carbon.GetEventParameter(ev, kEventParamMouseLocation,
                typeQDPoint, c_void_p(), sizeof(position), c_void_p(),
                byref(position)) 
            return carbon.FindWindow(position, None) == inContent 

    @CarbonEventHandler(kEventClassMouse, kEventMouseDown)
    def _on_mouse_down(self, next_handler, ev, data):
        x, y = self._get_mouse_position(ev)
        if self._get_mouse_in_content(ev, x, y):
            button = self._get_mouse_buttons_changed()
            modifiers = self._get_modifiers(ev)
            if button is not None:
                y = self.height - y
                self.dispatch_event('on_mouse_press', x, y, button, modifiers)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassMouse, kEventMouseUp)
    def _on_mouse_up(self, next_handler, ev, data):
        # Always report mouse up, even out of content area, because it's
        # probably after a drag gesture.
        button = self._get_mouse_buttons_changed()
        modifiers = self._get_modifiers(ev)
        if button is not None:
            x, y = self._get_mouse_position(ev)
            y = self.height - y
            self.dispatch_event('on_mouse_release', x, y, button, modifiers)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassMouse, kEventMouseMoved)
    def _on_mouse_moved(self, next_handler, ev, data):
        x, y = self._get_mouse_position(ev)
        if (self._get_mouse_in_content(ev, x, y)
            and not self._mouse_ignore_motion):
            y = self.height - y

            self._mouse_x = x
            self._mouse_y = y

            delta = HIPoint()
            carbon.GetEventParameter(ev, kEventParamMouseDelta,
                typeHIPoint, c_void_p(), sizeof(delta), c_void_p(),
                byref(delta))

            # Motion event
            self.dispatch_event('on_mouse_motion', 
                x, y, delta.x, -delta.y)
        elif self._mouse_ignore_motion:
            self._mouse_ignore_motion = False
        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassMouse, kEventMouseDragged)
    def _on_mouse_dragged(self, next_handler, ev, data):
        button = self._get_mouse_buttons()
        modifiers = self._get_modifiers(ev)
        if button is not None:
            x, y = self._get_mouse_position(ev)
            y = self.height - y

            self._mouse_x = x
            self._mouse_y = y

            delta = HIPoint()
            carbon.GetEventParameter(ev, kEventParamMouseDelta,
                typeHIPoint, c_void_p(), sizeof(delta), c_void_p(),
                byref(delta))

            # Drag event
            self.dispatch_event('on_mouse_drag',
                x, y, delta.x, -delta.y, button, modifiers)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassMouse, kEventMouseEntered)
    def _on_mouse_entered(self, next_handler, ev, data):
        x, y = self._get_mouse_position(ev)
        y = self.height - y

        self._mouse_x = x
        self._mouse_y = y
        self._mouse_in_window = True
        self.set_mouse_platform_visible()

        self.dispatch_event('on_mouse_enter', x, y)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassMouse, kEventMouseExited)
    def _on_mouse_exited(self, next_handler, ev, data):
        x, y = self._get_mouse_position(ev)
        y = self.height - y

        self._mouse_in_window = False
        self.set_mouse_platform_visible()

        self.dispatch_event('on_mouse_leave', x, y)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassMouse, kEventMouseWheelMoved)
    def _on_mouse_wheel_moved(self, next_handler, ev, data):

        x, y = self._get_mouse_position(ev)
        y = self.height - y

        axis = EventMouseWheelAxis()
        carbon.GetEventParameter(ev, kEventParamMouseWheelAxis,
            typeMouseWheelAxis, c_void_p(), sizeof(axis), c_void_p(),
            byref(axis))
        delta = c_long()
        carbon.GetEventParameter(ev, kEventParamMouseWheelDelta,
            typeSInt32, c_void_p(), sizeof(delta), c_void_p(),
            byref(delta))
        if axis.value == kEventMouseWheelAxisX:
            self.dispatch_event('on_mouse_scroll', 
                x, y, delta.value, 0)
        else:
            self.dispatch_event('on_mouse_scroll', 
                x, y, 0, delta.value)
                
        # _Don't_ call the next handler, which is application, as this then
        # calls our window handler again.
        #carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowClose)
    def _on_window_close(self, next_handler, ev, data):
        self.dispatch_event('on_close')

        # Presumably the next event handler is the one that closes
        # the window; don't do that here. 
        #carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowResizeStarted)
    def _on_window_resize_started(self, next_handler, ev, data):
        from pyglet import app
        if app.event_loop is not None:
            app.event_loop.enter_blocking()

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowResizeCompleted)
    def _on_window_resize_completed(self, next_handler, ev, data):
        from pyglet import app
        if app.event_loop is not None:
            app.event_loop.exit_blocking()

        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        width = rect.right - rect.left
        height = rect.bottom - rect.top

        self.switch_to()
        self.dispatch_event('on_resize', width, height)
        self.dispatch_event('on_expose')

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    _dragging = False

    @CarbonEventHandler(kEventClassWindow, kEventWindowDragStarted)
    def _on_window_drag_started(self, next_handler, ev, data):
        self._dragging = True

        from pyglet import app
        if app.event_loop is not None:
            app.event_loop.enter_blocking()

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowDragCompleted)
    def _on_window_drag_completed(self, next_handler, ev, data):
        self._dragging = False

        from pyglet import app
        if app.event_loop is not None:
            app.event_loop.exit_blocking()

        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))

        self.dispatch_event('on_move', rect.left, rect.top)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowBoundsChanging)
    def _on_window_bounds_changing(self, next_handler, ev, data):
        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowBoundsChanged)
    def _on_window_bounds_change(self, next_handler, ev, data):
        self._update_track_region()

        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        width = rect.right - rect.left
        height = rect.bottom - rect.top
        if width != self._width or height != self._height:
            self._update_drawable()
            self.switch_to()
            self.dispatch_event('on_resize', width, height)
            
            from pyglet import app
            if app.event_loop is not None:
                app.event_loop.enter_blocking()

            self._width = width
            self._height = height
        else:
            self.dispatch_event('on_move', rect.left, rect.top)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowZoomed)
    def _on_window_zoomed(self, next_handler, ev, data):
        rect = Rect()
        carbon.GetWindowBounds(self._window, kWindowContentRgn, byref(rect))
        width = rect.right - rect.left
        height = rect.bottom - rect.top

        self.dispatch_event('on_move', rect.left, rect.top)
        self.dispatch_event('on_resize', width, height)

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowActivated)
    def _on_window_activated(self, next_handler, ev, data):
        self.dispatch_event('on_activate')

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowDeactivated)
    def _on_window_deactivated(self, next_handler, ev, data):
        self.dispatch_event('on_deactivate')

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr
        
    @CarbonEventHandler(kEventClassWindow, kEventWindowShown)
    @CarbonEventHandler(kEventClassWindow, kEventWindowExpanded)
    def _on_window_shown(self, next_handler, ev, data):
        self._update_drawable() # XXX not needed here according to apple docs
        self.dispatch_event('on_show')

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowHidden)
    @CarbonEventHandler(kEventClassWindow, kEventWindowCollapsed)
    def _on_window_hidden(self, next_handler, ev, data):
        self.dispatch_event('on_hide')

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr

    @CarbonEventHandler(kEventClassWindow, kEventWindowDrawContent)
    def _on_window_draw_content(self, next_handler, ev, data):
        self.dispatch_event('on_expose')

        carbon.CallNextEventHandler(next_handler, ev)
        return noErr
        
CarbonWindow.register_event_type('on_recreate_immediate')
