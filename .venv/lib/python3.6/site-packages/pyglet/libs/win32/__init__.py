#!/usr/bin/python
# $Id: $

from __future__ import print_function
from __future__ import absolute_import
from builtins import object
import struct
from ctypes import *

import pyglet
from . import constants
from .types import *

IS64 = struct.calcsize("P") == 8

_debug_win32 = pyglet.options['debug_win32']

if _debug_win32:
    import traceback
    _GetLastError = windll.kernel32.GetLastError
    _SetLastError = windll.kernel32.SetLastError
    _FormatMessageA = windll.kernel32.FormatMessageA

    _log_win32 = open('debug_win32.log', 'w')
    
    def format_error(err):
        msg = create_string_buffer(256)
        _FormatMessageA(constants.FORMAT_MESSAGE_FROM_SYSTEM,
                        c_void_p(),
                        err,
                        0,
                        msg,
                        len(msg),
                        c_void_p())
        return msg.value
    
    class DebugLibrary(object):
        def __init__(self, lib):
            self.lib = lib

        def __getattr__(self, name):
            fn = getattr(self.lib, name)

            def f(*args):
                _SetLastError(0)
                result = fn(*args)
                err = _GetLastError()
                if err != 0:
                    for entry in traceback.format_list(traceback.extract_stack()[:-1]):
                        _log_win32.write(entry)
                    print(format_error(err), file=_log_win32)
                return result

            return f
else:
    DebugLibrary = lambda lib: lib


_gdi32 = DebugLibrary(windll.gdi32)
_kernel32 = DebugLibrary(windll.kernel32)
_user32 = DebugLibrary(windll.user32)

# _gdi32
_gdi32.AddFontMemResourceEx.restype = HANDLE
_gdi32.AddFontMemResourceEx.argtypes = [PVOID, DWORD, PVOID, POINTER(DWORD)]
_gdi32.ChoosePixelFormat.restype = c_int
_gdi32.ChoosePixelFormat.argtypes = [HDC, POINTER(PIXELFORMATDESCRIPTOR)]
_gdi32.CreateBitmap.restype = HBITMAP
_gdi32.CreateBitmap.argtypes = [c_int, c_int, UINT, UINT, c_void_p]
_gdi32.CreateCompatibleDC.restype = HDC
_gdi32.CreateCompatibleDC.argtypes = [HDC]
_gdi32.CreateDIBitmap.restype = HBITMAP
_gdi32.CreateDIBitmap.argtypes = [HDC, POINTER(BITMAPINFOHEADER), DWORD, c_void_p, POINTER(BITMAPINFO), UINT]
_gdi32.CreateDIBSection.restype = HBITMAP
_gdi32.CreateDIBSection.argtypes = [HDC, c_void_p, UINT, c_void_p, HANDLE, DWORD]  # POINTER(BITMAPINFO)
_gdi32.CreateFontIndirectA.restype = HFONT
_gdi32.CreateFontIndirectA.argtypes = [POINTER(LOGFONT)]
_gdi32.DeleteDC.restype = BOOL
_gdi32.DeleteDC.argtypes = [HDC]
_gdi32.DeleteObject.restype = BOOL
_gdi32.DeleteObject.argtypes = [HGDIOBJ]
_gdi32.DescribePixelFormat.restype = c_int
_gdi32.DescribePixelFormat.argtypes = [HDC, c_int, UINT, POINTER(PIXELFORMATDESCRIPTOR)]
_gdi32.ExtTextOutA.restype = BOOL
_gdi32.ExtTextOutA.argtypes = [HDC, c_int, c_int, UINT, LPRECT, c_char_p, UINT, POINTER(INT)]
_gdi32.GdiFlush.restype = BOOL
_gdi32.GdiFlush.argtypes = []
_gdi32.GetCharABCWidthsW.restype = BOOL
_gdi32.GetCharABCWidthsW.argtypes = [HDC, UINT, UINT, POINTER(ABC)]
_gdi32.GetCharWidth32W.restype = BOOL
_gdi32.GetCharWidth32W.argtypes = [HDC, UINT, UINT, POINTER(INT)]
_gdi32.GetStockObject.restype =  HGDIOBJ
_gdi32.GetStockObject.argtypes = [c_int]
_gdi32.GetTextMetricsA.restype = BOOL
_gdi32.GetTextMetricsA.argtypes = [HDC, POINTER(TEXTMETRIC)]
_gdi32.SelectObject.restype = HGDIOBJ
_gdi32.SelectObject.argtypes = [HDC, HGDIOBJ]
_gdi32.SetBkColor.restype = COLORREF
_gdi32.SetBkColor.argtypes = [HDC, COLORREF]
_gdi32.SetBkMode.restype = c_int
_gdi32.SetBkMode.argtypes = [HDC, c_int]
_gdi32.SetPixelFormat.restype = BOOL
_gdi32.SetPixelFormat.argtypes = [HDC, c_int, POINTER(PIXELFORMATDESCRIPTOR)]
_gdi32.SetTextColor.restype = COLORREF
_gdi32.SetTextColor.argtypes = [HDC, COLORREF]
_gdi32.SwapBuffers.restype = BOOL
_gdi32.SwapBuffers.argtypes = [HDC]

_kernel32.CloseHandle.restype = BOOL
_kernel32.CloseHandle.argtypes = [HANDLE]
_kernel32.CreateEventW.restype = HANDLE
_kernel32.CreateEventW.argtypes = [POINTER(SECURITY_ATTRIBUTES), BOOL, BOOL, c_wchar_p]
_kernel32.CreateWaitableTimerA.restype = HANDLE
_kernel32.CreateWaitableTimerA.argtypes = [POINTER(SECURITY_ATTRIBUTES), BOOL, c_char_p]
_kernel32.GetCurrentThreadId.restype = DWORD
_kernel32.GetCurrentThreadId.argtypes = []
_kernel32.GetModuleHandleW.restype = HMODULE
_kernel32.GetModuleHandleW.argtypes = [c_wchar_p]
_kernel32.GlobalAlloc.restype = HGLOBAL
_kernel32.GlobalAlloc.argtypes = [UINT, c_size_t]
_kernel32.GlobalLock.restype = LPVOID
_kernel32.GlobalLock.argtypes = [HGLOBAL]
_kernel32.GlobalUnlock.restype = BOOL
_kernel32.GlobalUnlock.argtypes = [HGLOBAL]
_kernel32.SetLastError.restype = DWORD
_kernel32.SetLastError.argtypes = []
_kernel32.SetWaitableTimer.restype = BOOL
_kernel32.SetWaitableTimer.argtypes = [HANDLE, POINTER(LARGE_INTEGER), LONG, LPVOID, LPVOID, BOOL]  # TIMERAPCPROC
_kernel32.WaitForSingleObject.restype = DWORD
_kernel32.WaitForSingleObject.argtypes = [HANDLE, DWORD]

_user32.AdjustWindowRectEx.restype = BOOL
_user32.AdjustWindowRectEx.argtypes = [LPRECT, DWORD, BOOL, DWORD]
_user32.ChangeDisplaySettingsExW.restype = LONG
_user32.ChangeDisplaySettingsExW.argtypes = [c_wchar_p, POINTER(DEVMODE), HWND, DWORD, LPVOID]
_user32.ClientToScreen.restype = BOOL
_user32.ClientToScreen.argtypes = [HWND, LPPOINT]
_user32.ClipCursor.restype = BOOL
_user32.ClipCursor.argtypes = [LPRECT]
_user32.CreateIconIndirect.restype = HICON
_user32.CreateIconIndirect.argtypes = [POINTER(ICONINFO)]
_user32.CreateWindowExW.restype = HWND
_user32.CreateWindowExW.argtypes = [DWORD, c_wchar_p, c_wchar_p, DWORD, c_int, c_int, c_int, c_int, HWND, HMENU, HINSTANCE, LPVOID]
_user32.DefWindowProcW.restype = LRESULT
_user32.DefWindowProcW.argtypes = [HWND, UINT, WPARAM, LPARAM]
_user32.DestroyWindow.restype = BOOL
_user32.DestroyWindow.argtypes = [HWND]
_user32.DispatchMessageW.restype = LRESULT
_user32.DispatchMessageW.argtypes = [LPMSG]
_user32.EnumDisplayMonitors.restype = BOOL
_user32.EnumDisplayMonitors.argtypes = [HDC, LPRECT, MONITORENUMPROC, LPARAM]
_user32.EnumDisplaySettingsW.restype = BOOL
_user32.EnumDisplaySettingsW.argtypes = [c_wchar_p, DWORD, POINTER(DEVMODE)]
_user32.FillRect.restype = c_int
_user32.FillRect.argtypes = [HDC, LPRECT, HBRUSH]
_user32.GetClientRect.restype = BOOL
_user32.GetClientRect.argtypes = [HWND, LPRECT]
_user32.GetCursorPos.restype = BOOL
_user32.GetCursorPos.argtypes = [LPPOINT]
# workaround for win 64-bit, see issue #664
_user32.GetDC.restype = c_void_p # HDC
_user32.GetDC.argtypes = [c_void_p] # [HWND]
_user32.GetDesktopWindow.restype = HWND
_user32.GetDesktopWindow.argtypes = []
_user32.GetKeyState.restype = c_short
_user32.GetKeyState.argtypes = [c_int]
_user32.GetMessageW.restype = BOOL
_user32.GetMessageW.argtypes = [LPMSG, HWND, UINT, UINT]
_user32.GetMonitorInfoW.restype = BOOL
_user32.GetMonitorInfoW.argtypes = [HMONITOR, POINTER(MONITORINFOEX)]
_user32.GetQueueStatus.restype = DWORD
_user32.GetQueueStatus.argtypes = [UINT]
_user32.GetSystemMetrics.restype = c_int
_user32.GetSystemMetrics.argtypes = [c_int]
_user32.LoadCursorW.restype = HCURSOR
_user32.LoadCursorW.argtypes = [HINSTANCE, c_wchar_p]
_user32.LoadIconW.restype = HICON
_user32.LoadIconW.argtypes = [HINSTANCE, c_wchar_p]
_user32.MapVirtualKeyW.restype = UINT
_user32.MapVirtualKeyW.argtypes = [UINT, UINT]
_user32.MapWindowPoints.restype = c_int
_user32.MapWindowPoints.argtypes = [HWND, HWND, c_void_p, UINT]  # HWND, HWND, LPPOINT, UINT
_user32.MsgWaitForMultipleObjects.restype = DWORD
_user32.MsgWaitForMultipleObjects.argtypes = [DWORD, POINTER(HANDLE), BOOL, DWORD, DWORD]
_user32.PeekMessageW.restype = BOOL
_user32.PeekMessageW.argtypes = [LPMSG, HWND, UINT, UINT, UINT]
_user32.PostThreadMessageW.restype = BOOL
_user32.PostThreadMessageW.argtypes = [DWORD, UINT, WPARAM, LPARAM]
_user32.RegisterClassW.restype = ATOM
_user32.RegisterClassW.argtypes = [POINTER(WNDCLASS)]
_user32.RegisterHotKey.restype = BOOL
_user32.RegisterHotKey.argtypes = [HWND, c_int, UINT, UINT]
_user32.ReleaseCapture.restype = BOOL
_user32.ReleaseCapture.argtypes = []
# workaround for win 64-bit, see issue #664
_user32.ReleaseDC.restype = c_int32 # c_int
_user32.ReleaseDC.argtypes = [c_void_p, c_void_p] # [HWND, HDC]
_user32.ScreenToClient.restype = BOOL
_user32.ScreenToClient.argtypes = [HWND, LPPOINT]
_user32.SetCapture.restype = HWND
_user32.SetCapture.argtypes = [HWND]
_user32.SetClassLongW.restype = DWORD
_user32.SetClassLongW.argtypes = [HWND, c_int, LONG]
if IS64:
    _user32.SetClassLongPtrW.restype = ULONG
    _user32.SetClassLongPtrW.argtypes = [HWND, c_int, LONG_PTR]
else:
    _user32.SetClassLongPtrW = _user32.SetClassLongW
_user32.SetCursor.restype = HCURSOR
_user32.SetCursor.argtypes = [HCURSOR]
_user32.SetCursorPos.restype = BOOL
_user32.SetCursorPos.argtypes = [c_int, c_int]
_user32.SetFocus.restype = HWND
_user32.SetFocus.argtypes = [HWND]
_user32.SetForegroundWindow.restype = BOOL
_user32.SetForegroundWindow.argtypes = [HWND]
_user32.SetTimer.restype = UINT_PTR
_user32.SetTimer.argtypes = [HWND, UINT_PTR, UINT, TIMERPROC]
_user32.SetWindowLongW.restype = LONG
_user32.SetWindowLongW.argtypes = [HWND, c_int, LONG]
_user32.SetWindowPos.restype = BOOL
_user32.SetWindowPos.argtypes = [HWND, HWND, c_int, c_int, c_int, c_int, UINT]
_user32.SetWindowTextW.restype = BOOL
_user32.SetWindowTextW.argtypes = [HWND, c_wchar_p]
_user32.ShowCursor.restype = c_int
_user32.ShowCursor.argtypes = [BOOL]
_user32.ShowWindow.restype = BOOL
_user32.ShowWindow.argtypes = [HWND, c_int]
_user32.TrackMouseEvent.restype = BOOL
_user32.TrackMouseEvent.argtypes = [POINTER(TRACKMOUSEEVENT)]
_user32.TranslateMessage.restype = BOOL
_user32.TranslateMessage.argtypes = [LPMSG]
_user32.UnregisterClassW.restype = BOOL
_user32.UnregisterClassW.argtypes = [c_wchar_p, HINSTANCE]
_user32.UnregisterHotKey.restype = BOOL
_user32.UnregisterHotKey.argtypes = [HWND, c_int]
# Raw inputs
_user32.RegisterRawInputDevices.restype = BOOL
_user32.RegisterRawInputDevices.argtypes = [PCRAWINPUTDEVICE, UINT, UINT]
_user32.GetRawInputData.restype = UINT
_user32.GetRawInputData.argtypes = [HRAWINPUT, UINT, LPVOID, PUINT, UINT]

