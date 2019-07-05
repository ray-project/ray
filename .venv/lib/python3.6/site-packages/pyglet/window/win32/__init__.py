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
from builtins import chr

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

from ctypes import *
import unicodedata
import warnings

from pyglet import compat_platform
if compat_platform not in ('cygwin', 'win32'):
    raise ImportError('Not a win32 platform.')

import pyglet
from pyglet.window import BaseWindow, \
    WindowException, MouseCursor, DefaultMouseCursor, _PlatformEventHandler, \
    _ViewEventHandler
from pyglet.event import EventDispatcher
from pyglet.window import key
from pyglet.window import mouse

from pyglet.canvas.win32 import Win32Canvas

from pyglet.libs.win32 import _user32, _kernel32, _gdi32
from pyglet.libs.win32.constants import *
from pyglet.libs.win32.winkey import *
from pyglet.libs.win32.types import *

# symbol,ctrl -> motion mapping
_motion_map = {
    (key.UP, False):        key.MOTION_UP,
    (key.RIGHT, False):     key.MOTION_RIGHT,
    (key.DOWN, False):      key.MOTION_DOWN,
    (key.LEFT, False):      key.MOTION_LEFT,
    (key.RIGHT, True):      key.MOTION_NEXT_WORD,
    (key.LEFT, True):       key.MOTION_PREVIOUS_WORD,
    (key.HOME, False):      key.MOTION_BEGINNING_OF_LINE,
    (key.END, False):       key.MOTION_END_OF_LINE,
    (key.PAGEUP, False):    key.MOTION_PREVIOUS_PAGE,
    (key.PAGEDOWN, False):  key.MOTION_NEXT_PAGE,
    (key.HOME, True):       key.MOTION_BEGINNING_OF_FILE,
    (key.END, True):        key.MOTION_END_OF_FILE,
    (key.BACKSPACE, False): key.MOTION_BACKSPACE,
    (key.DELETE, False):    key.MOTION_DELETE,
}

class Win32MouseCursor(MouseCursor):
    drawable = False
    def __init__(self, cursor):
        self.cursor = cursor

# This is global state, we have to be careful not to set the same state twice,
# which will throw off the ShowCursor counter.
_win32_cursor_visible = True

Win32EventHandler = _PlatformEventHandler
ViewEventHandler = _ViewEventHandler

class Win32Window(BaseWindow):
    _window_class = None
    _hwnd = None
    _dc = None
    _wgl_context = None
    _tracking = False
    _hidden = False
    _has_focus = False

    _exclusive_keyboard = False
    _exclusive_keyboard_focus = True
    _exclusive_mouse = False
    _exclusive_mouse_focus = True
    _exclusive_mouse_screen = None
    _exclusive_mouse_lpos = None
    _exclusive_mouse_buttons = 0
    _mouse_platform_visible = True

    _ws_style = 0
    _ex_ws_style = 0
    _minimum_size = None
    _maximum_size = None

    def __init__(self, *args, **kwargs):
        # Bind event handlers
        self._event_handlers = {}
        self._view_event_handlers = {}
        for func_name in self._platform_event_names:
            if not hasattr(self, func_name):
                continue
            func = getattr(self, func_name)
            for message in func._platform_event_data:
                if hasattr(func, '_view'):
                    self._view_event_handlers[message] = func
                else:
                    self._event_handlers[message] = func


        super(Win32Window, self).__init__(*args, **kwargs)

    def _recreate(self, changes):
        if 'context' in changes:
            self._wgl_context = None

        self._create()

    def _create(self):
        # Ensure style is set before determining width/height.
        if self._fullscreen:
            self._ws_style = WS_POPUP
            self._ex_ws_style = 0 # WS_EX_TOPMOST
        else:
            styles = {
                self.WINDOW_STYLE_DEFAULT: (WS_OVERLAPPEDWINDOW, 0),
                self.WINDOW_STYLE_DIALOG:  (WS_OVERLAPPED|WS_CAPTION|WS_SYSMENU,
                                            WS_EX_DLGMODALFRAME),
                self.WINDOW_STYLE_TOOL:    (WS_OVERLAPPED|WS_CAPTION|WS_SYSMENU,
                                            WS_EX_TOOLWINDOW),
                self.WINDOW_STYLE_BORDERLESS: (WS_POPUP, 0),
            }
            self._ws_style, self._ex_ws_style = styles[self._style]

        if self._resizable and not self._fullscreen:
            self._ws_style |= WS_THICKFRAME
        else:
            self._ws_style &= ~(WS_THICKFRAME|WS_MAXIMIZEBOX)


        if self._fullscreen:
            width = self.screen.width
            height = self.screen.height
        else:
            width, height = \
                self._client_to_window_size(self._width, self._height)

        if not self._window_class:
            module = _kernel32.GetModuleHandleW(None)
            white = _gdi32.GetStockObject(WHITE_BRUSH)
            black = _gdi32.GetStockObject(BLACK_BRUSH)
            self._window_class = WNDCLASS()
            self._window_class.lpszClassName = u'GenericAppClass%d' % id(self)
            self._window_class.lpfnWndProc = WNDPROC(
                self._get_window_proc(self._event_handlers))
            self._window_class.style = CS_VREDRAW | CS_HREDRAW
            self._window_class.hInstance = 0
            self._window_class.hIcon = _user32.LoadIconW(module, MAKEINTRESOURCE(1))
            self._window_class.hbrBackground = black
            self._window_class.lpszMenuName = None
            self._window_class.cbClsExtra = 0
            self._window_class.cbWndExtra = 0
            _user32.RegisterClassW(byref(self._window_class))

            self._view_window_class = WNDCLASS()
            self._view_window_class.lpszClassName = \
                u'GenericViewClass%d' % id(self)
            self._view_window_class.lpfnWndProc = WNDPROC(
                self._get_window_proc(self._view_event_handlers))
            self._view_window_class.style = 0
            self._view_window_class.hInstance = 0
            self._view_window_class.hIcon = 0
            self._view_window_class.hbrBackground = white
            self._view_window_class.lpszMenuName = None
            self._view_window_class.cbClsExtra = 0
            self._view_window_class.cbWndExtra = 0
            _user32.RegisterClassW(byref(self._view_window_class))

        if not self._hwnd:
            self._hwnd = _user32.CreateWindowExW(
                self._ex_ws_style,
                self._window_class.lpszClassName,
                u'',
                self._ws_style,
                CW_USEDEFAULT,
                CW_USEDEFAULT,
                width,
                height,
                0,
                0,
                self._window_class.hInstance,
                0)

            self._view_hwnd = _user32.CreateWindowExW(
                0,
                self._view_window_class.lpszClassName,
                u'',
                WS_CHILD | WS_VISIBLE,
                0, 0, 0, 0,
                self._hwnd,
                0,
                self._view_window_class.hInstance,
                0)


            self._dc = _user32.GetDC(self._view_hwnd)
        else:
            # Window already exists, update it with new style

            # We need to hide window here, otherwise Windows forgets
            # to redraw the whole screen after leaving fullscreen.
            _user32.ShowWindow(self._hwnd, SW_HIDE)

            _user32.SetWindowLongW(self._hwnd,
                GWL_STYLE,
                self._ws_style)
            _user32.SetWindowLongW(self._hwnd,
                GWL_EXSTYLE,
                self._ex_ws_style)

        if self._fullscreen:
            hwnd_after = HWND_TOPMOST
        else:
            hwnd_after = HWND_NOTOPMOST

        # Position and size window
        if self._fullscreen:
            _user32.SetWindowPos(self._hwnd, hwnd_after,
                self._screen.x, self._screen.y, width, height, SWP_FRAMECHANGED)
        elif False: # TODO location not in pyglet API
            x, y = self._client_to_window_pos(*factory.get_location())
            _user32.SetWindowPos(self._hwnd, hwnd_after,
                x, y, width, height, SWP_FRAMECHANGED)
        else:
            _user32.SetWindowPos(self._hwnd, hwnd_after,
                0, 0, width, height, SWP_NOMOVE | SWP_FRAMECHANGED)

        self._update_view_location(self._width, self._height)

        # Context must be created after window is created.
        if not self._wgl_context:
            self.canvas = Win32Canvas(self.display, self._view_hwnd, self._dc)
            self.context.attach(self.canvas)
            self._wgl_context = self.context._context

        self.set_caption(self._caption)

        self.switch_to()
        self.set_vsync(self._vsync)

        if self._visible:
            self.set_visible()
            # Might need resize event if going from fullscreen to fullscreen
            self.dispatch_event('on_resize', self._width, self._height)
            self.dispatch_event('on_expose')

    def _update_view_location(self, width, height):
        if self._fullscreen:
            x = (self.screen.width - width) // 2
            y = (self.screen.height - height) // 2
        else:
            x = y = 0
        _user32.SetWindowPos(self._view_hwnd, 0,
            x, y, width, height, SWP_NOZORDER | SWP_NOOWNERZORDER)

    def close(self):
        if not self._hwnd:
            super(Win32Window, self).close()
            return

        _user32.DestroyWindow(self._hwnd)
        _user32.UnregisterClassW(self._window_class.lpszClassName, 0)
        
        self._window_class = None
        self._view_window_class = None
        self._view_event_handlers.clear()
        self._event_handlers.clear()
        self.set_mouse_platform_visible(True)
        self._hwnd = None
        self._dc = None
        self._wgl_context = None
        super(Win32Window, self).close()

    def _get_vsync(self):
        return self.context.get_vsync()
    vsync = property(_get_vsync) # overrides BaseWindow property

    def set_vsync(self, vsync):
        if pyglet.options['vsync'] is not None:
            vsync = pyglet.options['vsync']
        self.context.set_vsync(vsync)

    def switch_to(self):
        self.context.set_current()

    def flip(self):
        self.draw_mouse_cursor()
        self.context.flip()

    def set_location(self, x, y):
        x, y = self._client_to_window_pos(x, y)
        _user32.SetWindowPos(self._hwnd, 0, x, y, 0, 0,
            (SWP_NOZORDER |
             SWP_NOSIZE |
             SWP_NOOWNERZORDER))

    def get_location(self):
        rect = RECT()
        _user32.GetClientRect(self._hwnd, byref(rect))
        point = POINT()
        point.x = rect.left
        point.y = rect.top
        _user32.ClientToScreen(self._hwnd, byref(point))
        return point.x, point.y

    def set_size(self, width, height):
        if self._fullscreen:
            raise WindowException('Cannot set size of fullscreen window.')
        width, height = self._client_to_window_size(width, height)
        _user32.SetWindowPos(self._hwnd, 0, 0, 0, width, height,
            (SWP_NOZORDER |
             SWP_NOMOVE |
             SWP_NOOWNERZORDER))

    def get_size(self):
        #rect = RECT()
        #_user32.GetClientRect(self._hwnd, byref(rect))
        #return rect.right - rect.left, rect.bottom - rect.top
        return self._width, self._height

    def set_minimum_size(self, width, height):
        self._minimum_size = width, height

    def set_maximum_size(self, width, height):
        self._maximum_size = width, height

    def activate(self):
        _user32.SetForegroundWindow(self._hwnd)

    def set_visible(self, visible=True):
        if visible:
            insertAfter = HWND_TOPMOST if self._fullscreen else HWND_TOP
            _user32.SetWindowPos(self._hwnd, insertAfter, 0, 0, 0, 0,
                SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW)
            self.dispatch_event('on_resize', self._width, self._height)
            self.activate()
            self.dispatch_event('on_show')
        else:
            _user32.ShowWindow(self._hwnd, SW_HIDE)
            self.dispatch_event('on_hide')
        self._visible = visible
        self.set_mouse_platform_visible()

    def minimize(self):
        _user32.ShowWindow(self._hwnd, SW_MINIMIZE)

    def maximize(self):
        _user32.ShowWindow(self._hwnd, SW_MAXIMIZE)

    def set_caption(self, caption):
        self._caption = caption
        _user32.SetWindowTextW(self._hwnd, c_wchar_p(caption))

    def set_mouse_platform_visible(self, platform_visible=None):
        if platform_visible is None:
            platform_visible = (self._mouse_visible and
                                not self._exclusive_mouse and
                                not self._mouse_cursor.drawable) or \
                               (not self._mouse_in_window or
                                not self._has_focus)

        if platform_visible and not self._mouse_cursor.drawable:
            if isinstance(self._mouse_cursor, Win32MouseCursor):
                cursor = self._mouse_cursor.cursor
            else:
                cursor = _user32.LoadCursorW(None, MAKEINTRESOURCE(IDC_ARROW))
            _user32.SetClassLongW(self._view_hwnd, GCL_HCURSOR, cursor)
            _user32.SetCursor(cursor)

        if platform_visible == self._mouse_platform_visible:
            return

        # Avoid calling ShowCursor with the current visibility (which would
        # push the counter too far away from zero).
        global _win32_cursor_visible
        if _win32_cursor_visible != platform_visible:
            _user32.ShowCursor(platform_visible)
            _win32_cursor_visible = platform_visible

        self._mouse_platform_visible = platform_visible

    def _reset_exclusive_mouse_screen(self):
        '''Recalculate screen coords of mouse warp point for exclusive
        mouse.'''
        p = POINT()
        rect = RECT()
        _user32.GetClientRect(self._view_hwnd, byref(rect))
        _user32.MapWindowPoints(self._view_hwnd, HWND_DESKTOP, byref(rect), 2)
        p.x = (rect.left + rect.right) // 2
        p.y = (rect.top + rect.bottom) // 2

        # This is the point the mouse will be kept at while in exclusive
        # mode.
        self._exclusive_mouse_screen = p.x, p.y

    def set_exclusive_mouse(self, exclusive=True):
        if self._exclusive_mouse == exclusive and \
           self._exclusive_mouse_focus == self._has_focus:
            return

        # Mouse: UsagePage = 1, Usage = 2
        raw_mouse = RAWINPUTDEVICE(0x01, 0x02, 0, None)
        if exclusive:
            raw_mouse.dwFlags = RIDEV_NOLEGACY
            raw_mouse.hwndTarget = self._view_hwnd
        else:
            raw_mouse.dwFlags = RIDEV_REMOVE
            raw_mouse.hwndTarget = None

        if not _user32.RegisterRawInputDevices(
                byref(raw_mouse), 1, sizeof(RAWINPUTDEVICE)):
            if exclusive:
                raise WindowException("Cannot enter mouse exclusive mode.")

        self._exclusive_mouse_buttons = 0
        if exclusive and self._has_focus:
            # Clip to client area, to prevent large mouse movements taking
            # it outside the client area.
            rect = RECT()
            _user32.GetClientRect(self._view_hwnd, byref(rect))
            _user32.MapWindowPoints(self._view_hwnd, HWND_DESKTOP,
                                    byref(rect), 2)
            _user32.ClipCursor(byref(rect))
            # Release mouse capture in case is was acquired during mouse click
            _user32.ReleaseCapture()
        else:
            # Release clip
            _user32.ClipCursor(None)

        self._exclusive_mouse = exclusive
        self._exclusive_mouse_focus = self._has_focus
        self.set_mouse_platform_visible(not exclusive)

    def set_mouse_position(self, x, y, absolute=False):
        if not absolute:
            rect = RECT()
            _user32.GetClientRect(self._view_hwnd, byref(rect))
            _user32.MapWindowPoints(self._view_hwnd, HWND_DESKTOP, byref(rect), 2)

            x = x + rect.left
            y = rect.top + (rect.bottom - rect.top) - y

        _user32.SetCursorPos(x, y)

    def set_exclusive_keyboard(self, exclusive=True):
        if self._exclusive_keyboard == exclusive and \
           self._exclusive_keyboard_focus == self._has_focus:
            return

        if exclusive and self._has_focus:
            _user32.RegisterHotKey(self._hwnd, 0, WIN32_MOD_ALT, VK_TAB)
        else:
            _user32.UnregisterHotKey(self._hwnd, 0)

        self._exclusive_keyboard = exclusive
        self._exclusive_keyboard_focus = self._has_focus

    def get_system_mouse_cursor(self, name):
        if name == self.CURSOR_DEFAULT:
            return DefaultMouseCursor()

        names = {
            self.CURSOR_CROSSHAIR:       IDC_CROSS,
            self.CURSOR_HAND:            IDC_HAND,
            self.CURSOR_HELP:            IDC_HELP,
            self.CURSOR_NO:              IDC_NO,
            self.CURSOR_SIZE:            IDC_SIZEALL,
            self.CURSOR_SIZE_UP:         IDC_SIZENS,
            self.CURSOR_SIZE_UP_RIGHT:   IDC_SIZENESW,
            self.CURSOR_SIZE_RIGHT:      IDC_SIZEWE,
            self.CURSOR_SIZE_DOWN_RIGHT: IDC_SIZENWSE,
            self.CURSOR_SIZE_DOWN:       IDC_SIZENS,
            self.CURSOR_SIZE_DOWN_LEFT:  IDC_SIZENESW,
            self.CURSOR_SIZE_LEFT:       IDC_SIZEWE,
            self.CURSOR_SIZE_UP_LEFT:    IDC_SIZENWSE,
            self.CURSOR_SIZE_UP_DOWN:    IDC_SIZENS,
            self.CURSOR_SIZE_LEFT_RIGHT: IDC_SIZEWE,
            self.CURSOR_TEXT:            IDC_IBEAM,
            self.CURSOR_WAIT:            IDC_WAIT,
            self.CURSOR_WAIT_ARROW:      IDC_APPSTARTING,
        }
        if name not in names:
            raise RuntimeError('Unknown cursor name "%s"' % name)
        cursor = _user32.LoadCursorW(None, MAKEINTRESOURCE(names[name]))
        return Win32MouseCursor(cursor)

    def set_icon(self, *images):
        # XXX Undocumented AFAICT, but XP seems happy to resize an image
        # of any size, so no scaling necessary.

        def best_image(width, height):
            # A heuristic for finding closest sized image to required size.
            image = images[0]
            for img in images:
                if img.width == width and img.height == height:
                    # Exact match always used
                    return img
                elif img.width >= width and \
                     img.width * img.height > image.width * image.height:
                    # At least wide enough, and largest area
                    image = img
            return image

        def get_icon(image):
            # Alpha-blended icon: see http://support.microsoft.com/kb/318876
            format = 'BGRA'
            pitch = len(format) * image.width

            header = BITMAPV5HEADER()
            header.bV5Size = sizeof(header)
            header.bV5Width = image.width
            header.bV5Height = image.height
            header.bV5Planes = 1
            header.bV5BitCount = 32
            header.bV5Compression = BI_BITFIELDS
            header.bV5RedMask = 0x00ff0000
            header.bV5GreenMask = 0x0000ff00
            header.bV5BlueMask = 0x000000ff
            header.bV5AlphaMask = 0xff000000

            hdc = _user32.GetDC(None)
            dataptr = c_void_p()
            bitmap = _gdi32.CreateDIBSection(hdc, byref(header), DIB_RGB_COLORS,
                byref(dataptr), None, 0)
            _user32.ReleaseDC(None, hdc)

            image = image.get_image_data()
            data = image.get_data(format, pitch)
            memmove(dataptr, data, len(data))

            mask = _gdi32.CreateBitmap(image.width, image.height, 1, 1, None)

            iconinfo = ICONINFO()
            iconinfo.fIcon = True
            iconinfo.hbmMask = mask
            iconinfo.hbmColor = bitmap
            icon = _user32.CreateIconIndirect(byref(iconinfo))

            _gdi32.DeleteObject(mask)
            _gdi32.DeleteObject(bitmap)

            return icon

        # Set large icon
        image = best_image(_user32.GetSystemMetrics(SM_CXICON),
                           _user32.GetSystemMetrics(SM_CYICON))
        icon = get_icon(image)
        _user32.SetClassLongPtrW(self._hwnd, GCL_HICON, icon)

        # Set small icon
        image = best_image(_user32.GetSystemMetrics(SM_CXSMICON),
                           _user32.GetSystemMetrics(SM_CYSMICON))
        icon = get_icon(image)
        _user32.SetClassLongPtrW(self._hwnd, GCL_HICONSM, icon)

    # Private util

    def _client_to_window_size(self, width, height):
        rect = RECT()
        rect.left = 0
        rect.top = 0
        rect.right = width
        rect.bottom = height
        _user32.AdjustWindowRectEx(byref(rect),
            self._ws_style, False, self._ex_ws_style)
        return rect.right - rect.left, rect.bottom - rect.top

    def _client_to_window_pos(self, x, y):
        rect = RECT()
        rect.left = x
        rect.top = y
        _user32.AdjustWindowRectEx(byref(rect),
            self._ws_style, False, self._ex_ws_style)
        return rect.left, rect.top

    # Event dispatching

    def dispatch_events(self):
        from pyglet import app
        app.platform_event_loop.start()
        self._allow_dispatch_event = True
        self.dispatch_pending_events()

        msg = MSG()
        while _user32.PeekMessageW(byref(msg), 0, 0, 0, PM_REMOVE):
            _user32.TranslateMessage(byref(msg))
            _user32.DispatchMessageW(byref(msg))
        self._allow_dispatch_event = False

    def dispatch_pending_events(self):
        while self._event_queue:
            event = self._event_queue.pop(0)
            if type(event[0]) is str:
                # pyglet event
                EventDispatcher.dispatch_event(self, *event)
            else:
                # win32 event
                event[0](*event[1:])

    def _get_window_proc(self, event_handlers):
        def f(hwnd, msg, wParam, lParam):
            event_handler = event_handlers.get(msg, None)
            result = None
            if event_handler:
                if self._allow_dispatch_event or not self._enable_event_queue:
                    result = event_handler(msg, wParam, lParam)
                else:
                    result = 0
                    self._event_queue.append((event_handler, msg,
                                              wParam, lParam))
            if result is None:
                result = _user32.DefWindowProcW(hwnd, msg, wParam, lParam)
            return result

        return f

    # Event handlers

    def _get_modifiers(self, key_lParam=0):
        modifiers = 0
        if _user32.GetKeyState(VK_SHIFT) & 0xff00:
            modifiers |= key.MOD_SHIFT
        if _user32.GetKeyState(VK_CONTROL) & 0xff00:
            modifiers |= key.MOD_CTRL
        if _user32.GetKeyState(VK_LWIN) & 0xff00:
            modifiers |= key.MOD_WINDOWS
        if _user32.GetKeyState(VK_CAPITAL) & 0x00ff:    # toggle
            modifiers |= key.MOD_CAPSLOCK
        if _user32.GetKeyState(VK_NUMLOCK) & 0x00ff:    # toggle
            modifiers |= key.MOD_NUMLOCK
        if _user32.GetKeyState(VK_SCROLL) & 0x00ff:    # toggle
            modifiers |= key.MOD_SCROLLLOCK
        if key_lParam:
            if key_lParam & (1 << 29):
                modifiers |= key.MOD_ALT
        elif _user32.GetKeyState(VK_MENU) < 0:
            modifiers |= key.MOD_ALT
        return modifiers

    @staticmethod
    def _get_location(lParam):
        x = c_int16(lParam & 0xffff).value
        y = c_int16(lParam >> 16).value
        return x, y

    @Win32EventHandler(WM_KEYDOWN)
    @Win32EventHandler(WM_KEYUP)
    @Win32EventHandler(WM_SYSKEYDOWN)
    @Win32EventHandler(WM_SYSKEYUP)
    def _event_key(self, msg, wParam, lParam):
        repeat = False
        if lParam & (1 << 30):
            if msg not in (WM_KEYUP, WM_SYSKEYUP):
                repeat = True
            ev = 'on_key_release'
        else:
            ev = 'on_key_press'

        symbol = keymap.get(wParam, None)
        if symbol is None:
            ch = _user32.MapVirtualKeyW(wParam, MAPVK_VK_TO_CHAR)
            symbol = chmap.get(ch)

        if symbol is None:
            symbol = key.user_key(wParam)
        elif symbol == key.LCTRL and lParam & (1 << 24):
            symbol = key.RCTRL
        elif symbol == key.LALT and lParam & (1 << 24):
            symbol = key.RALT
        elif symbol == key.LSHIFT:
            pass # TODO: some magic with getstate to find out if it's the
                 # right or left shift key.

        modifiers = self._get_modifiers(lParam)

        if not repeat:
            self.dispatch_event(ev, symbol, modifiers)

        ctrl = modifiers & key.MOD_CTRL != 0
        if (symbol, ctrl) in _motion_map and msg not in (WM_KEYUP, WM_SYSKEYUP):
            motion = _motion_map[symbol, ctrl]
            if modifiers & key.MOD_SHIFT:
                self.dispatch_event('on_text_motion_select', motion)
            else:
                self.dispatch_event('on_text_motion', motion)

        # Send on to DefWindowProc if not exclusive.
        if self._exclusive_keyboard:
            return 0
        else:
            return None

    @Win32EventHandler(WM_CHAR)
    def _event_char(self, msg, wParam, lParam):
        text = chr(wParam)
        if unicodedata.category(text) != 'Cc' or text == '\r':
            self.dispatch_event('on_text', text)
        return 0

    @ViewEventHandler
    @Win32EventHandler(WM_INPUT)
    def _event_raw_input(self, msg, wParam, lParam):
        if not self._exclusive_mouse:
            return 0

        hRawInput = cast(lParam, HRAWINPUT)
        inp = RAWINPUT()
        size = UINT(sizeof(inp))
        _user32.GetRawInputData(hRawInput, RID_INPUT, byref(inp),
                        byref(size), sizeof(RAWINPUTHEADER))

        if inp.header.dwType == RIM_TYPEMOUSE:
            rmouse = inp.data.mouse

            if rmouse.usButtonFlags & RI_MOUSE_LEFT_BUTTON_DOWN:
                self.dispatch_event('on_mouse_press', 0, 0, mouse.LEFT,
                                    self._get_modifiers())
                self._exclusive_mouse_buttons |= mouse.LEFT
            if rmouse.usButtonFlags & RI_MOUSE_LEFT_BUTTON_UP:
                self.dispatch_event('on_mouse_release', 0, 0, mouse.LEFT,
                                    self._get_modifiers())
                self._exclusive_mouse_buttons &= ~mouse.LEFT
            if rmouse.usButtonFlags & RI_MOUSE_RIGHT_BUTTON_DOWN:
                self.dispatch_event('on_mouse_press', 0, 0, mouse.RIGHT,
                                    self._get_modifiers())
                self._exclusive_mouse_buttons |= mouse.RIGHT
            if rmouse.usButtonFlags & RI_MOUSE_RIGHT_BUTTON_UP:
                self.dispatch_event('on_mouse_release', 0, 0, mouse.RIGHT,
                                    self._get_modifiers())
                self._exclusive_mouse_buttons &= ~mouse.RIGHT
            if rmouse.usButtonFlags & RI_MOUSE_MIDDLE_BUTTON_DOWN:
                self.dispatch_event('on_mouse_press', 0, 0, mouse.MIDDLE,
                                    self._get_modifiers())
                self._exclusive_mouse_buttons |= mouse.MIDDLE
            if rmouse.usButtonFlags & RI_MOUSE_MIDDLE_BUTTON_UP:
                self.dispatch_event('on_mouse_release', 0, 0, mouse.MIDDLE,
                                    self._get_modifiers())
                self._exclusive_mouse_buttons &= ~mouse.MIDDLE
            if rmouse.usButtonFlags & RI_MOUSE_WHEEL:
                delta = SHORT(rmouse.usButtonData).value
                self.dispatch_event('on_mouse_scroll',
                    0, 0, 0, delta / float(WHEEL_DELTA))

            if rmouse.usFlags & 0x01 == MOUSE_MOVE_RELATIVE:
                if rmouse.lLastX != 0 or rmouse.lLastY != 0:
                    # Motion event
                    # In relative motion, Y axis is positive for below.
                    # We invert it for Pyglet so positive is motion up.
                    if self._exclusive_mouse_buttons:
                        self.dispatch_event('on_mouse_drag', 0, 0,
                                            rmouse.lLastX, -rmouse.lLastY,
                                            self._exclusive_mouse_buttons,
                                            self._get_modifiers())
                    else:
                        self.dispatch_event('on_mouse_motion', 0, 0,
                                            rmouse.lLastX, -rmouse.lLastY)
            else:
                if self._exclusive_mouse_lpos is None:
                    self._exclusive_mouse_lpos = rmouse.lLastX, rmouse.lLastY
                last_x, last_y = self._exclusive_mouse_lpos
                rel_x = rmouse.lLastX - last_x
                rel_y = rmouse.lLastY - last_y
                if rel_x != 0 or rel_y != 0.0:
                    # Motion event
                    if self._exclusive_mouse_buttons:
                        self.dispatch_event('on_mouse_drag', 0, 0,
                                            rmouse.lLastX, -rmouse.lLastY,
                                            self._exclusive_mouse_buttons,
                                            self._get_modifiers())
                    else:
                        self.dispatch_event('on_mouse_motion', 0, 0,
                                        rel_x, rel_y)
                    self._exclusive_mouse_lpos = rmouse.lLastX, rmouse.lLastY

        return 0

    @ViewEventHandler
    @Win32EventHandler(WM_MOUSEMOVE)
    def _event_mousemove(self, msg, wParam, lParam):
        if self._exclusive_mouse and self._has_focus:
            return 0

        x, y = self._get_location(lParam)
        y = self._height - y

        dx = x - self._mouse_x
        dy = y - self._mouse_y

        if not self._tracking:
            # There is no WM_MOUSEENTER message (!), so fake it from the
            # first WM_MOUSEMOVE event after leaving.  Use self._tracking
            # to determine when to recreate the tracking structure after
            # re-entering (to track the next WM_MOUSELEAVE).
            self._mouse_in_window = True
            self.set_mouse_platform_visible()
            self.dispatch_event('on_mouse_enter', x, y)
            self._tracking = True
            track = TRACKMOUSEEVENT()
            track.cbSize = sizeof(track)
            track.dwFlags = TME_LEAVE
            track.hwndTrack = self._view_hwnd
            _user32.TrackMouseEvent(byref(track))

        # Don't generate motion/drag events when mouse hasn't moved. (Issue
        # 305)
        if self._mouse_x == x and self._mouse_y == y:
            return 0

        self._mouse_x = x
        self._mouse_y = y

        buttons = 0
        if wParam & MK_LBUTTON:
            buttons |= mouse.LEFT
        if wParam & MK_MBUTTON:
            buttons |= mouse.MIDDLE
        if wParam & MK_RBUTTON:
            buttons |= mouse.RIGHT

        if buttons:
            # Drag event
            modifiers = self._get_modifiers()
            self.dispatch_event('on_mouse_drag',
                x, y, dx, dy, buttons, modifiers)
        else:
            # Motion event
            self.dispatch_event('on_mouse_motion', x, y, dx, dy)
        return 0

    @ViewEventHandler
    @Win32EventHandler(WM_MOUSELEAVE)
    def _event_mouseleave(self, msg, wParam, lParam):
        point = POINT()
        _user32.GetCursorPos(byref(point))
        _user32.ScreenToClient(self._view_hwnd, byref(point))
        x = point.x
        y = self._height - point.y
        self._tracking = False
        self._mouse_in_window = False
        self.set_mouse_platform_visible()
        self.dispatch_event('on_mouse_leave', x, y)
        return 0

    def _event_mousebutton(self, ev, button, lParam):
        if ev == 'on_mouse_press':
            _user32.SetCapture(self._view_hwnd)
        else:
            _user32.ReleaseCapture()
        x, y = self._get_location(lParam)
        y = self._height - y
        self.dispatch_event(ev, x, y, button, self._get_modifiers())
        return 0

    @ViewEventHandler
    @Win32EventHandler(WM_LBUTTONDOWN)
    def _event_lbuttondown(self, msg, wParam, lParam):
        return self._event_mousebutton(
            'on_mouse_press', mouse.LEFT, lParam)

    @ViewEventHandler
    @Win32EventHandler(WM_LBUTTONUP)
    def _event_lbuttonup(self, msg, wParam, lParam):
        return self._event_mousebutton(
            'on_mouse_release', mouse.LEFT, lParam)

    @ViewEventHandler
    @Win32EventHandler(WM_MBUTTONDOWN)
    def _event_mbuttondown(self, msg, wParam, lParam):
        return self._event_mousebutton(
            'on_mouse_press', mouse.MIDDLE, lParam)

    @ViewEventHandler
    @Win32EventHandler(WM_MBUTTONUP)
    def _event_mbuttonup(self, msg, wParam, lParam):
        return self._event_mousebutton(
            'on_mouse_release', mouse.MIDDLE, lParam)

    @ViewEventHandler
    @Win32EventHandler(WM_RBUTTONDOWN)
    def _event_rbuttondown(self, msg, wParam, lParam):
        return self._event_mousebutton(
            'on_mouse_press', mouse.RIGHT, lParam)

    @ViewEventHandler
    @Win32EventHandler(WM_RBUTTONUP)
    def _event_rbuttonup(self, msg, wParam, lParam):
        return self._event_mousebutton(
            'on_mouse_release', mouse.RIGHT, lParam)

    @Win32EventHandler(WM_MOUSEWHEEL)
    def _event_mousewheel(self, msg, wParam, lParam):
        delta = c_short(wParam >> 16).value
        self.dispatch_event('on_mouse_scroll',
            self._mouse_x, self._mouse_y, 0, delta / float(WHEEL_DELTA))
        return 0

    @Win32EventHandler(WM_CLOSE)
    def _event_close(self, msg, wParam, lParam):
        self.dispatch_event('on_close')
        return 0

    @ViewEventHandler
    @Win32EventHandler(WM_PAINT)
    def _event_paint(self, msg, wParam, lParam):
        self.dispatch_event('on_expose')

        # Validating the window using ValidateRect or ValidateRgn
        # doesn't clear the paint message when more than one window
        # is open [why?]; defer to DefWindowProc instead.
        return None

    @Win32EventHandler(WM_SIZING)
    def _event_sizing(self, msg, wParam, lParam):
        # rect = cast(lParam, POINTER(RECT)).contents
        # width, height = self.get_size()

        from pyglet import app
        if app.event_loop is not None:
            app.event_loop.enter_blocking()
        return 1

    @Win32EventHandler(WM_SIZE)
    def _event_size(self, msg, wParam, lParam):
        if not self._dc:
            # Ignore window creation size event (appears for fullscreen
            # only) -- we haven't got DC or HWND yet.
            return None

        if wParam == SIZE_MINIMIZED:
            # Minimized, not resized.
            self._hidden = True
            self.dispatch_event('on_hide')
            return 0
        if self._hidden:
            # Restored
            self._hidden = False
            self.dispatch_event('on_show')
        w, h = self._get_location(lParam)
        if not self._fullscreen:
            self._width, self._height = w, h
        self._update_view_location(self._width, self._height)
        self.switch_to()
        self.dispatch_event('on_resize', self._width, self._height)
        return 0

    @Win32EventHandler(WM_SYSCOMMAND)
    def _event_syscommand(self, msg, wParam, lParam):
        # check for ALT key to prevent app from hanging because there is
        # no windows menu bar
        if wParam == SC_KEYMENU and lParam & (1 >> 16) <= 0:
            return 0

        if wParam & 0xfff0 in (SC_MOVE, SC_SIZE):
            # Should be in WM_ENTERSIZEMOVE, but we never get that message.
            from pyglet import app

            if app.event_loop is not None:
                app.event_loop.enter_blocking()

    @Win32EventHandler(WM_MOVE)
    def _event_move(self, msg, wParam, lParam):
        x, y = self._get_location(lParam)
        self.dispatch_event('on_move', x, y)
        return 0

    @Win32EventHandler(WM_EXITSIZEMOVE)
    def _event_entersizemove(self, msg, wParam, lParam):
        from pyglet import app
        if app.event_loop is not None:
            app.event_loop.exit_blocking()

    '''
    # Alternative to using WM_SETFOCUS and WM_KILLFOCUS.  Which
    # is better?

    @Win32EventHandler(WM_ACTIVATE)
    def _event_activate(self, msg, wParam, lParam):
        if wParam & 0xffff == WA_INACTIVE:
            self.dispatch_event('on_deactivate')
        else:
            self.dispatch_event('on_activate')
            _user32.SetFocus(self._hwnd)
        return 0
    '''

    @Win32EventHandler(WM_SETFOCUS)
    def _event_setfocus(self, msg, wParam, lParam):
        self.dispatch_event('on_activate')
        self._has_focus = True

        self.set_exclusive_keyboard(self._exclusive_keyboard)
        self.set_exclusive_mouse(self._exclusive_mouse)
        return 0

    @Win32EventHandler(WM_KILLFOCUS)
    def _event_killfocus(self, msg, wParam, lParam):
        self.dispatch_event('on_deactivate')
        self._has_focus = False
        exclusive_keyboard = self._exclusive_keyboard
        exclusive_mouse = self._exclusive_mouse
        # Disable both exclusive keyboard and mouse
        self.set_exclusive_keyboard(False)
        self.set_exclusive_mouse(False)

        # But save desired state and note that we lost focus
        # This will allow to reset the correct mode once we regain focus
        self._exclusive_keyboard = exclusive_keyboard
        self._exclusive_keyboard_focus = False
        self._exclusive_mouse = exclusive_mouse
        self._exclusive_mouse_focus = False
        return 0

    @Win32EventHandler(WM_GETMINMAXINFO)
    def _event_getminmaxinfo(self, msg, wParam, lParam):
        info = MINMAXINFO.from_address(lParam)
        if self._minimum_size:
            info.ptMinTrackSize.x, info.ptMinTrackSize.y = \
                self._client_to_window_size(*self._minimum_size)
        if self._maximum_size:
            info.ptMaxTrackSize.x, info.ptMaxTrackSize.y = \
                self._client_to_window_size(*self._maximum_size)
        return 0

    @Win32EventHandler(WM_ERASEBKGND)
    def _event_erasebkgnd(self, msg, wParam, lParam):
        # Prevent flicker during resize; but erase bkgnd if we're fullscreen.
        if self._fullscreen:
            return 0
        else:
            return 1

    @ViewEventHandler
    @Win32EventHandler(WM_ERASEBKGND)
    def _event_erasebkgnd_view(self, msg, wParam, lParam):
        # Prevent flicker during resize.
        return 1
