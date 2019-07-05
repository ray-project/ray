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

"""
"""

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import sys
import ctypes
from ctypes import *
from ctypes.wintypes import *


_int_types = (c_int16, c_int32)
if hasattr(ctypes, 'c_int64'):
    # Some builds of ctypes apparently do not have c_int64
    # defined; it's a pretty good bet that these builds do not
    # have 64-bit pointers.
    _int_types += (c_int64,)
for t in _int_types:
    if sizeof(t) == sizeof(c_size_t):
        c_ptrdiff_t = t
del t
del _int_types


# PUINT is defined only from >= python 3.2
if sys.version_info < (3, 2)[:2]:
    PUINT = POINTER(UINT)


class c_void(Structure):
    # c_void_p is a buggy return type, converting to int, so
    # POINTER(None) == c_void_p is actually written as
    # POINTER(c_void), so it can be treated as a real pointer.
    _fields_ = [('dummy', c_int)]


def POINTER_(obj):
    p = ctypes.POINTER(obj)

    # Convert None to a real NULL pointer to work around bugs
    # in how ctypes handles None on 64-bit platforms
    if not isinstance(p.from_param, classmethod):
        def from_param(cls, x):
            if x is None:
                return cls()
            else:
                return x
        p.from_param = classmethod(from_param)

    return p


c_void_p = POINTER_(c_void)
INT = c_int
LPVOID = c_void_p
HCURSOR = HANDLE
LRESULT = LPARAM
COLORREF = DWORD
PVOID = c_void_p
WCHAR = c_wchar
BCHAR = c_wchar
LPRECT = POINTER(RECT)
LPPOINT = POINTER(POINT)
LPMSG = POINTER(MSG)
UINT_PTR = HANDLE
LONG_PTR = HANDLE
HDROP = HANDLE
LPTSTR = LPWSTR

LF_FACESIZE = 32
CCHDEVICENAME = 32
CCHFORMNAME = 32

WNDPROC = WINFUNCTYPE(LRESULT, HWND, UINT, WPARAM, LPARAM)
TIMERPROC = WINFUNCTYPE(None, HWND, UINT, POINTER(UINT), DWORD)
TIMERAPCPROC = WINFUNCTYPE(None, PVOID, DWORD, DWORD)
MONITORENUMPROC = WINFUNCTYPE(BOOL, HMONITOR, HDC, LPRECT, LPARAM)


def MAKEINTRESOURCE(i):
    return cast(ctypes.c_void_p(i & 0xFFFF), c_wchar_p)


class WNDCLASS(Structure):
    _fields_ = [
        ('style', UINT),
        ('lpfnWndProc', WNDPROC),
        ('cbClsExtra', c_int),
        ('cbWndExtra', c_int),
        ('hInstance', HINSTANCE),
        ('hIcon', HICON),
        ('hCursor', HCURSOR),
        ('hbrBackground', HBRUSH),
        ('lpszMenuName', c_char_p),
        ('lpszClassName', c_wchar_p)
    ]


class SECURITY_ATTRIBUTES(Structure):
    _fields_ = [
        ("nLength", DWORD),
        ("lpSecurityDescriptor", c_void_p),
        ("bInheritHandle", BOOL)
    ]
    __slots__ = [f[0] for f in _fields_]


class PIXELFORMATDESCRIPTOR(Structure):
    _fields_ = [
        ('nSize', WORD),
        ('nVersion', WORD),
        ('dwFlags', DWORD),
        ('iPixelType', BYTE),
        ('cColorBits', BYTE),
        ('cRedBits', BYTE),
        ('cRedShift', BYTE),
        ('cGreenBits', BYTE),
        ('cGreenShift', BYTE),
        ('cBlueBits', BYTE),
        ('cBlueShift', BYTE),
        ('cAlphaBits', BYTE),
        ('cAlphaShift', BYTE),
        ('cAccumBits', BYTE),
        ('cAccumRedBits', BYTE),
        ('cAccumGreenBits', BYTE),
        ('cAccumBlueBits', BYTE),
        ('cAccumAlphaBits', BYTE),
        ('cDepthBits', BYTE),
        ('cStencilBits', BYTE),
        ('cAuxBuffers', BYTE),
        ('iLayerType', BYTE),
        ('bReserved', BYTE),
        ('dwLayerMask', DWORD),
        ('dwVisibleMask', DWORD),
        ('dwDamageMask', DWORD)
    ]


class RGBQUAD(Structure):
    _fields_ = [
        ('rgbBlue', BYTE),
        ('rgbGreen', BYTE),
        ('rgbRed', BYTE),
        ('rgbReserved', BYTE),
    ]
    __slots__ = [f[0] for f in _fields_]


class CIEXYZ(Structure):
    _fields_ = [
        ('ciexyzX', DWORD),
        ('ciexyzY', DWORD),
        ('ciexyzZ', DWORD),
    ]
    __slots__ = [f[0] for f in _fields_]


class CIEXYZTRIPLE(Structure):
    _fields_ = [
        ('ciexyzRed', CIEXYZ),
        ('ciexyzBlue', CIEXYZ),
        ('ciexyzGreen', CIEXYZ),
    ]
    __slots__ = [f[0] for f in _fields_]


class BITMAPINFOHEADER(Structure):
    _fields_ = [
        ('biSize', DWORD),
        ('biWidth', LONG),
        ('biHeight', LONG),
        ('biPlanes', WORD),
        ('biBitCount', WORD),
        ('biCompression', DWORD),
        ('biSizeImage', DWORD),
        ('biXPelsPerMeter', LONG),
        ('biYPelsPerMeter', LONG),
        ('biClrUsed', DWORD),
        ('biClrImportant', DWORD),
    ]


class BITMAPV5HEADER(Structure):
    _fields_ = [
        ('bV5Size', DWORD),
        ('bV5Width', LONG),
        ('bV5Height', LONG),
        ('bV5Planes', WORD),
        ('bV5BitCount', WORD),
        ('bV5Compression', DWORD),
        ('bV5SizeImage', DWORD),
        ('bV5XPelsPerMeter', LONG),
        ('bV5YPelsPerMeter', LONG),
        ('bV5ClrUsed', DWORD),
        ('bV5ClrImportant', DWORD),
        ('bV5RedMask', DWORD),
        ('bV5GreenMask', DWORD),
        ('bV5BlueMask', DWORD),
        ('bV5AlphaMask', DWORD),
        ('bV5CSType', DWORD),
        ('bV5Endpoints', CIEXYZTRIPLE),
        ('bV5GammaRed', DWORD),
        ('bV5GammaGreen', DWORD),
        ('bV5GammaBlue', DWORD),
        ('bV5Intent', DWORD),
        ('bV5ProfileData', DWORD),
        ('bV5ProfileSize', DWORD),
        ('bV5Reserved', DWORD),
    ]


class BITMAPINFO(Structure):
    _fields_ = [
        ('bmiHeader', BITMAPINFOHEADER),
        ('bmiColors', RGBQUAD * 1)
    ]
    __slots__ = [f[0] for f in _fields_]


class LOGFONT(Structure):
    _fields_ = [
        ('lfHeight', LONG),
        ('lfWidth', LONG),
        ('lfEscapement', LONG),
        ('lfOrientation', LONG),
        ('lfWeight', LONG),
        ('lfItalic', BYTE),
        ('lfUnderline', BYTE),
        ('lfStrikeOut', BYTE),
        ('lfCharSet', BYTE),
        ('lfOutPrecision', BYTE),
        ('lfClipPrecision', BYTE),
        ('lfQuality', BYTE),
        ('lfPitchAndFamily', BYTE),
        ('lfFaceName', (c_char * LF_FACESIZE))  # Use ASCII
    ]


class TRACKMOUSEEVENT(Structure):
    _fields_ = [
        ('cbSize', DWORD),
        ('dwFlags', DWORD),
        ('hwndTrack', HWND),
        ('dwHoverTime', DWORD)
    ]
    __slots__ = [f[0] for f in _fields_]


class MINMAXINFO(Structure):
    _fields_ = [
        ('ptReserved', POINT),
        ('ptMaxSize', POINT),
        ('ptMaxPosition', POINT),
        ('ptMinTrackSize', POINT),
        ('ptMaxTrackSize', POINT)
    ]
    __slots__ = [f[0] for f in _fields_]


class ABC(Structure):
    _fields_ = [
        ('abcA', c_int),
        ('abcB', c_uint),
        ('abcC', c_int)
    ]
    __slots__ = [f[0] for f in _fields_]


class TEXTMETRIC(Structure):
    _fields_ = [
        ('tmHeight', c_long),
        ('tmAscent', c_long),
        ('tmDescent', c_long),
        ('tmInternalLeading', c_long),
        ('tmExternalLeading', c_long),
        ('tmAveCharWidth', c_long),
        ('tmMaxCharWidth', c_long),
        ('tmWeight', c_long),
        ('tmOverhang', c_long),
        ('tmDigitizedAspectX', c_long),
        ('tmDigitizedAspectY', c_long),
        ('tmFirstChar', c_char),  # Use ASCII
        ('tmLastChar', c_char),
        ('tmDefaultChar', c_char),
        ('tmBreakChar', c_char),
        ('tmItalic', c_byte),
        ('tmUnderlined', c_byte),
        ('tmStruckOut', c_byte),
        ('tmPitchAndFamily', c_byte),
        ('tmCharSet', c_byte)
    ]
    __slots__ = [f[0] for f in _fields_]


class MONITORINFOEX(Structure):
    _fields_ = [
        ('cbSize', DWORD),
        ('rcMonitor', RECT),
        ('rcWork', RECT),
        ('dwFlags', DWORD),
        ('szDevice', WCHAR * CCHDEVICENAME)
    ]
    __slots__ = [f[0] for f in _fields_]


class DEVMODE(Structure):
    _fields_ = [
        ('dmDeviceName', BCHAR * CCHDEVICENAME),
        ('dmSpecVersion', WORD),
        ('dmDriverVersion', WORD),
        ('dmSize', WORD),
        ('dmDriverExtra', WORD),
        ('dmFields', DWORD),
        # Just using largest union member here
        ('dmOrientation', c_short),
        ('dmPaperSize', c_short),
        ('dmPaperLength', c_short),
        ('dmPaperWidth', c_short),
        ('dmScale', c_short),
        ('dmCopies', c_short),
        ('dmDefaultSource', c_short),
        ('dmPrintQuality', c_short),
        # End union
        ('dmColor', c_short),
        ('dmDuplex', c_short),
        ('dmYResolution', c_short),
        ('dmTTOption', c_short),
        ('dmCollate', c_short),
        ('dmFormName', BCHAR * CCHFORMNAME),
        ('dmLogPixels', WORD),
        ('dmBitsPerPel', DWORD),
        ('dmPelsWidth', DWORD),
        ('dmPelsHeight', DWORD),
        ('dmDisplayFlags', DWORD), # union with dmNup
        ('dmDisplayFrequency', DWORD),
        ('dmICMMethod', DWORD),
        ('dmICMIntent', DWORD),
        ('dmDitherType', DWORD),
        ('dmReserved1', DWORD),
        ('dmReserved2', DWORD),
        ('dmPanningWidth', DWORD),
        ('dmPanningHeight', DWORD),
    ]


class ICONINFO(Structure):
    _fields_ = [
        ('fIcon', BOOL),
        ('xHotspot', DWORD),
        ('yHotspot', DWORD),
        ('hbmMask', HBITMAP),
        ('hbmColor', HBITMAP)
    ]
    __slots__ = [f[0] for f in _fields_]


class RAWINPUTDEVICE(Structure):
    _fields_ = [
        ('usUsagePage', USHORT),
        ('usUsage', USHORT),
        ('dwFlags', DWORD),
        ('hwndTarget', HWND)
    ]


PCRAWINPUTDEVICE = POINTER(RAWINPUTDEVICE)
HRAWINPUT = HANDLE


class RAWINPUTHEADER(Structure):
    _fields_ = [
        ('dwType', DWORD),
        ('dwSize', DWORD),
        ('hDevice', HANDLE),
        ('wParam', WPARAM),
    ]


class _Buttons(Structure):
    _fields_ = [
        ('usButtonFlags', USHORT),
        ('usButtonData', USHORT),
    ]


class _U(Union):
    _anonymous_ = ('_buttons',)
    _fields_ = [
        ('ulButtons', ULONG),
        ('_buttons', _Buttons),
    ]


class RAWMOUSE(Structure):
    _anonymous_ = ('u',)
    _fields_ = [
        ('usFlags', USHORT),
        ('u', _U),
        ('ulRawButtons', ULONG),
        ('lLastX', LONG),
        ('lLastY', LONG),
        ('ulExtraInformation', ULONG),
    ]


class RAWKEYBOARD(Structure):
    _fields_ = [
        ('MakeCode', USHORT),
        ('Flags', USHORT),
        ('Reserved', USHORT),
        ('VKey', USHORT),
        ('Message', UINT),
        ('ExtraInformation', ULONG),
    ]


class RAWHID(Structure):
    _fields_ = [
        ('dwSizeHid', DWORD),
        ('dwCount', DWORD),
        ('bRawData', POINTER(BYTE)),
    ]


class _RAWINPUTDEVICEUNION(Union):
    _fields_ = [
        ('mouse', RAWMOUSE),
        ('keyboard', RAWKEYBOARD),
        ('hid', RAWHID),
    ]


class RAWINPUT(Structure):
    _fields_ = [
        ('header', RAWINPUTHEADER),
        ('data', _RAWINPUTDEVICEUNION),
    ]