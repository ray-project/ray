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
from builtins import range

__docformat__ = 'restructuredtext'
__version__ = '$Id: pil.py 163 2006-11-13 04:15:46Z Alex.Holkner $'

from ctypes import *

from pyglet.com import IUnknown
from pyglet.gl import *
from pyglet.image import *
from pyglet.image.codecs import *
from pyglet.libs.win32.constants import *
from pyglet.libs.win32.types import *
from pyglet.libs.win32 import _kernel32 as kernel32


ole32 = windll.ole32
gdiplus = windll.gdiplus

LPSTREAM = c_void_p
REAL = c_float

PixelFormat1bppIndexed    = 196865
PixelFormat4bppIndexed    = 197634
PixelFormat8bppIndexed    = 198659
PixelFormat16bppGrayScale = 1052676
PixelFormat16bppRGB555    = 135173
PixelFormat16bppRGB565    = 135174
PixelFormat16bppARGB1555  = 397319
PixelFormat24bppRGB       = 137224
PixelFormat32bppRGB       = 139273
PixelFormat32bppARGB      = 2498570
PixelFormat32bppPARGB     = 925707
PixelFormat48bppRGB       = 1060876
PixelFormat64bppARGB      = 3424269
PixelFormat64bppPARGB     = 29622286
PixelFormatMax            = 15

ImageLockModeRead = 1
ImageLockModeWrite = 2
ImageLockModeUserInputBuf = 4

class GdiplusStartupInput(Structure):
    _fields_ = [
        ('GdiplusVersion', c_uint32),
        ('DebugEventCallback', c_void_p),
        ('SuppressBackgroundThread', BOOL),
        ('SuppressExternalCodecs', BOOL)
    ]

class GdiplusStartupOutput(Structure):
    _fields = [
        ('NotificationHookProc', c_void_p),
        ('NotificationUnhookProc', c_void_p)
    ]

class BitmapData(Structure):
    _fields_ = [
        ('Width', c_uint),
        ('Height', c_uint),
        ('Stride', c_int),
        ('PixelFormat', c_int),
        ('Scan0', POINTER(c_byte)),
        ('Reserved', POINTER(c_uint))
    ]

class Rect(Structure):
    _fields_ = [
        ('X', c_int),
        ('Y', c_int),
        ('Width', c_int),
        ('Height', c_int)
    ]

PropertyTagFrameDelay = 0x5100

class PropertyItem(Structure):
    _fields_ = [
        ('id', c_uint),
        ('length', c_ulong),
        ('type', c_short),
        ('value', c_void_p)
    ]
    
INT_PTR = POINTER(INT)
UINT_PTR = POINTER(UINT)

ole32.CreateStreamOnHGlobal.argtypes = [HGLOBAL, BOOL, LPSTREAM]

gdiplus.GdipBitmapLockBits.restype = c_int
gdiplus.GdipBitmapLockBits.argtypes = [c_void_p, c_void_p, UINT, c_int, c_void_p]
gdiplus.GdipBitmapUnlockBits.restype = c_int
gdiplus.GdipBitmapUnlockBits.argtypes = [c_void_p, c_void_p]
gdiplus.GdipCloneStringFormat.restype = c_int
gdiplus.GdipCloneStringFormat.argtypes = [c_void_p, c_void_p]
gdiplus.GdipCreateBitmapFromScan0.restype = c_int
gdiplus.GdipCreateBitmapFromScan0.argtypes = [c_int, c_int, c_int, c_int, POINTER(BYTE), c_void_p]
gdiplus.GdipCreateBitmapFromStream.restype = c_int
gdiplus.GdipCreateBitmapFromStream.argtypes = [c_void_p, c_void_p]
gdiplus.GdipCreateFont.restype = c_int
gdiplus.GdipCreateFont.argtypes = [c_void_p, REAL, INT, c_int, c_void_p]
gdiplus.GdipCreateFontFamilyFromName.restype = c_int
gdiplus.GdipCreateFontFamilyFromName.argtypes = [c_wchar_p, c_void_p, c_void_p]
gdiplus.GdipCreateMatrix.restype = None
gdiplus.GdipCreateMatrix.argtypes = [c_void_p]
gdiplus.GdipCreateSolidFill.restype = c_int
gdiplus.GdipCreateSolidFill.argtypes = [c_int, c_void_p]  # ARGB
gdiplus.GdipDisposeImage.restype = c_int
gdiplus.GdipDisposeImage.argtypes = [c_void_p]
gdiplus.GdipDrawString.restype = c_int
gdiplus.GdipDrawString.argtypes = [c_void_p, c_wchar_p, c_int, c_void_p, c_void_p, c_void_p, c_void_p]
gdiplus.GdipGetFamilyName.restype = c_int
gdiplus.GdipGetFamilyName.argtypes = [c_void_p, c_wchar_p, c_wchar]
gdiplus.GdipFlush.restype = c_int
gdiplus.GdipFlush.argtypes = [c_void_p, c_int]
gdiplus.GdipGetFontCollectionFamilyCount.restype = c_int
gdiplus.GdipGetFontCollectionFamilyCount.argtypes = [c_void_p, INT_PTR]
gdiplus.GdipGetFontCollectionFamilyList.restype = c_int
gdiplus.GdipGetFontCollectionFamilyList.argtypes = [c_void_p, INT, c_void_p, INT_PTR]
gdiplus.GdipGetImageDimension.restype = c_int
gdiplus.GdipGetImageDimension.argtypes = [c_void_p, POINTER(REAL), POINTER(REAL)]
gdiplus.GdipGetImageGraphicsContext.restype = c_int
gdiplus.GdipGetImageGraphicsContext.argtypes = [c_void_p, c_void_p]
gdiplus.GdipGetImagePixelFormat.restype = c_int
gdiplus.GdipGetImagePixelFormat.argtypes = [c_void_p, c_void_p]
gdiplus.GdipGetPropertyItem.restype = c_int
gdiplus.GdipGetPropertyItem.argtypes = [c_void_p, c_uint, c_uint, c_void_p]
gdiplus.GdipGetPropertyItemSize.restype = c_int
gdiplus.GdipGetPropertyItemSize.argtypes = [c_void_p, c_uint, UINT_PTR]
gdiplus.GdipGraphicsClear.restype = c_int
gdiplus.GdipGraphicsClear.argtypes = [c_void_p, c_int]  # ARGB
gdiplus.GdipImageGetFrameCount.restype = c_int
gdiplus.GdipImageGetFrameCount.argtypes = [c_void_p, c_void_p, UINT_PTR]
gdiplus.GdipImageGetFrameDimensionsCount.restype = c_int
gdiplus.GdipImageGetFrameDimensionsCount.argtypes = [c_void_p, UINT_PTR]
gdiplus.GdipImageGetFrameDimensionsList.restype = c_int
gdiplus.GdipImageGetFrameDimensionsList.argtypes = [c_void_p, c_void_p, UINT]
gdiplus.GdipImageSelectActiveFrame.restype = c_int
gdiplus.GdipImageSelectActiveFrame.argtypes = [c_void_p, c_void_p, UINT]
gdiplus.GdipMeasureString.restype = c_int
gdiplus.GdipMeasureString.argtypes = [c_void_p, c_wchar_p, c_int, c_void_p, c_void_p, c_void_p, c_void_p, INT_PTR, INT_PTR]
gdiplus.GdipNewPrivateFontCollection.restype = c_int
gdiplus.GdipNewPrivateFontCollection.argtypes = [c_void_p]
gdiplus.GdipPrivateAddMemoryFont.restype = c_int
gdiplus.GdipPrivateAddMemoryFont.argtypes = [c_void_p, c_void_p, c_int]
gdiplus.GdipSetPageUnit.restype = c_int
gdiplus.GdipSetPageUnit.argtypes = [c_void_p, c_int]
gdiplus.GdipSetStringFormatFlags.restype = c_int
gdiplus.GdipSetStringFormatFlags.argtypes = [c_void_p, c_int]
gdiplus.GdipSetTextRenderingHint.restype = c_int
gdiplus.GdipSetTextRenderingHint.argtypes = [c_void_p, c_int]
gdiplus.GdipStringFormatGetGenericTypographic.restype = c_int
gdiplus.GdipStringFormatGetGenericTypographic.argtypes = [c_void_p]
gdiplus.GdiplusShutdown.restype = None
gdiplus.GdiplusShutdown.argtypes = [POINTER(ULONG)]
gdiplus.GdiplusStartup.restype = c_int
gdiplus.GdiplusStartup.argtypes = [c_void_p, c_void_p, c_void_p]


class GDIPlusDecoder(ImageDecoder):
    def get_file_extensions(self):
        return ['.bmp', '.gif', '.jpg', '.jpeg', '.exif', '.png', '.tif', 
                '.tiff']

    def get_animation_file_extensions(self):
        # TIFF also supported as a multi-page image; but that's not really an
        # animation, is it?
        return ['.gif']

    def _load_bitmap(self, file, filename):
        data = file.read()

        # Create a HGLOBAL with image data
        hglob = kernel32.GlobalAlloc(GMEM_MOVEABLE, len(data))
        ptr = kernel32.GlobalLock(hglob)
        memmove(ptr, data, len(data))
        kernel32.GlobalUnlock(hglob)

        # Create IStream for the HGLOBAL
        self.stream = IUnknown()
        ole32.CreateStreamOnHGlobal(hglob, True, byref(self.stream))

        # Load image from stream
        bitmap = c_void_p()
        status = gdiplus.GdipCreateBitmapFromStream(self.stream, byref(bitmap))
        if status != 0:
            self.stream.Release()
            raise ImageDecodeException(
                'GDI+ cannot load %r' % (filename or file))

        return bitmap

    def _get_image(self, bitmap):
        # Get size of image (Bitmap subclasses Image)
        width = REAL()
        height = REAL()
        gdiplus.GdipGetImageDimension(bitmap, byref(width), byref(height))
        width = int(width.value)
        height = int(height.value)

        # Get image pixel format
        pf = c_int()
        gdiplus.GdipGetImagePixelFormat(bitmap, byref(pf))
        pf = pf.value

        # Reverse from what's documented because of Intel little-endianness.
        format = 'BGRA'
        if pf == PixelFormat24bppRGB:
            format = 'BGR'
        elif pf == PixelFormat32bppRGB:
            pass
        elif pf == PixelFormat32bppARGB:
            pass
        elif pf in (PixelFormat16bppARGB1555, PixelFormat32bppPARGB,
                    PixelFormat64bppARGB, PixelFormat64bppPARGB):
            pf = PixelFormat32bppARGB
        else:
            format = 'BGR'
            pf = PixelFormat24bppRGB

        # Lock pixel data in best format
        rect = Rect()
        rect.X = 0
        rect.Y = 0
        rect.Width = width
        rect.Height = height
        bitmap_data = BitmapData()
        gdiplus.GdipBitmapLockBits(bitmap, 
            byref(rect), ImageLockModeRead, pf, byref(bitmap_data))
        
        # Create buffer for RawImage
        buffer = create_string_buffer(bitmap_data.Stride * height)
        memmove(buffer, bitmap_data.Scan0, len(buffer))
        
        # Unlock data
        gdiplus.GdipBitmapUnlockBits(bitmap, byref(bitmap_data))

        return ImageData(width, height, format, buffer, -bitmap_data.Stride)

    def _delete_bitmap(self, bitmap):
        # Release image and stream
        gdiplus.GdipDisposeImage(bitmap)
        self.stream.Release()

    def decode(self, file, filename):
        bitmap = self._load_bitmap(file, filename)
        image = self._get_image(bitmap)
        self._delete_bitmap(bitmap)
        return image

    def decode_animation(self, file, filename):
        bitmap = self._load_bitmap(file, filename)
        
        dimension_count = c_uint()
        gdiplus.GdipImageGetFrameDimensionsCount(bitmap, byref(dimension_count))
        if dimension_count.value < 1:
            self._delete_bitmap(bitmap)
            raise ImageDecodeException('Image has no frame dimensions')
        
        # XXX Make sure this dimension is time?
        dimensions = (c_void_p * dimension_count.value)()
        gdiplus.GdipImageGetFrameDimensionsList(bitmap, dimensions,
                                                dimension_count.value)

        frame_count = c_uint()
        gdiplus.GdipImageGetFrameCount(bitmap, dimensions, byref(frame_count))

        prop_id = PropertyTagFrameDelay
        prop_size = c_uint()
        gdiplus.GdipGetPropertyItemSize(bitmap, prop_id, byref(prop_size))

        prop_buffer = c_buffer(prop_size.value)
        prop_item = cast(prop_buffer, POINTER(PropertyItem)).contents 
        gdiplus.GdipGetPropertyItem(bitmap, prop_id, prop_size.value,
            prop_buffer)

        n_delays = prop_item.length // sizeof(c_long)
        delays = cast(prop_item.value, POINTER(c_long * n_delays)).contents

        frames = []
        
        for i in range(frame_count.value):
            gdiplus.GdipImageSelectActiveFrame(bitmap, dimensions, i)
            image = self._get_image(bitmap)

            delay = delays[i]
            if delay <= 1:
                delay = 10
            frames.append(AnimationFrame(image, delay/100.))

        self._delete_bitmap(bitmap)

        return Animation(frames)

def get_decoders():
    return [GDIPlusDecoder()]

def get_encoders():
    return []

def init():
    token = c_ulong()
    startup_in = GdiplusStartupInput()
    startup_in.GdiplusVersion = 1
    startup_out = GdiplusStartupOutput()
    gdiplus.GdiplusStartup(byref(token), byref(startup_in), byref(startup_out))

    # Shutdown later?
    # gdiplus.GdiplusShutdown(token)

init()
