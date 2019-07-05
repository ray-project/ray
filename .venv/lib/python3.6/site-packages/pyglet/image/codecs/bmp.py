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

'''Decoder for BMP files.

Currently supports version 3 and 4 bitmaps with BI_RGB and BI_BITFIELDS
encoding.  Alpha channel is supported for 32-bit BI_RGB only.
'''
from builtins import range

# Official docs are at
# http://msdn2.microsoft.com/en-us/library/ms532311.aspx
#
# But some details including alignment and bit/byte order are omitted; see
# http://www.fileformat.info/format/bmp/egff.htm

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

import ctypes

from pyglet.image import ImageData
from pyglet.image.codecs import ImageDecoder, ImageDecodeException

BYTE = ctypes.c_ubyte
WORD = ctypes.c_uint16
DWORD = ctypes.c_uint32
LONG = ctypes.c_int32
FXPT2DOT30 = ctypes.c_uint32

BI_RGB = 0
BI_RLE8 = 1
BI_RLE4 = 2
BI_BITFIELDS = 3

class BITMAPFILEHEADER(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('bfType', WORD),
        ('bfSize', DWORD),
        ('bfReserved1', WORD),
        ('bfReserved2', WORD),
        ('bfOffBits', DWORD)
    ]

class BITMAPINFOHEADER(ctypes.LittleEndianStructure):
    _pack_ = 1
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
        ('biClrImportant', DWORD)
    ]

CIEXYZTRIPLE = FXPT2DOT30 * 9

class BITMAPV4HEADER(ctypes.LittleEndianStructure):
    _pack_ = 1
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
        ('bV4RedMask', DWORD),
        ('bV4GreenMask', DWORD),
        ('bV4BlueMask', DWORD),
        ('bV4AlphaMask', DWORD),
        ('bV4CSType', DWORD),
        ('bV4Endpoints', CIEXYZTRIPLE),
        ('bV4GammaRed', DWORD),
        ('bV4GammaGreen', DWORD),
        ('bV4GammaBlue', DWORD),
    ]

class RGBFields(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('red', DWORD),
        ('green', DWORD),
        ('blue', DWORD),
    ]


class RGBQUAD(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('rgbBlue', BYTE),
        ('rgbGreen', BYTE),
        ('rgbRed', BYTE),
        ('rgbReserved', BYTE)
    ]

    def __repr__(self):
        return '<%d, %d, %d>' % (self.rgbRed, self.rgbGreen, self.rgbBlue)

def ptr_add(ptr, offset):
    address = ctypes.addressof(ptr.contents) + offset
    return ctypes.pointer(type(ptr.contents).from_address(address))

def to_ctypes(buffer, offset, type):
    if offset + ctypes.sizeof(type) > len(buffer):
        raise ImageDecodeException('BMP file is truncated')
    ptr = ptr_add(ctypes.pointer(buffer), offset)
    return ctypes.cast(ptr, ctypes.POINTER(type)).contents

class BMPImageDecoder(ImageDecoder):
    def get_file_extensions(self):
        return ['.bmp']

    def decode(self, file, filename):
        if not file:
            file = open(filename, 'rb')
        bytes = file.read()
        buffer = ctypes.c_buffer(bytes)

        if bytes[:2] != b'BM':
            raise ImageDecodeException(
                'Not a Windows bitmap file: %r' % (filename or file))

        file_header = to_ctypes(buffer, 0, BITMAPFILEHEADER)
        bits_offset = file_header.bfOffBits
        info_header_offset = ctypes.sizeof(BITMAPFILEHEADER)
        info_header = to_ctypes(buffer, info_header_offset, BITMAPINFOHEADER)
        palette_offset = info_header_offset + info_header.biSize

        if info_header.biSize < ctypes.sizeof(BITMAPINFOHEADER):
            raise ImageDecodeException(
                'Unsupported BMP type: %r' % (filename or file))

        width = info_header.biWidth
        height = info_header.biHeight
        if width <= 0  or info_header.biPlanes != 1:
            raise ImageDecodeException(
                'BMP file has corrupt parameters: %r' % (filename or file))
        pitch_sign = height < 0 and -1 or 1
        height = abs(height)

        compression = info_header.biCompression
        if compression not in (BI_RGB, BI_BITFIELDS):
            raise ImageDecodeException(
                'Unsupported compression: %r' % (filename or file))

        clr_used = 0
        bitcount = info_header.biBitCount 
        if bitcount == 1:
            pitch = (width + 7) // 8
            bits_type = ctypes.c_ubyte
            decoder = decode_1bit
        elif bitcount == 4:
            pitch = (width + 1) // 2
            bits_type = ctypes.c_ubyte
            decoder = decode_4bit
        elif bitcount == 8:
            bits_type = ctypes.c_ubyte
            pitch = width
            decoder = decode_8bit
        elif bitcount == 16:
            pitch = width * 2
            bits_type = ctypes.c_uint16
            decoder = decode_bitfields
        elif bitcount == 24:
            pitch = width * 3
            bits_type = ctypes.c_ubyte
            decoder = decode_24bit 
        elif bitcount == 32:
            pitch = width * 4
            if compression == BI_RGB:
                decoder = decode_32bit_rgb
                bits_type = ctypes.c_ubyte
            elif compression == BI_BITFIELDS:
                decoder = decode_bitfields
                bits_type = ctypes.c_uint32
            else:
                raise ImageDecodeException(
                    'Unsupported compression: %r' % (filename or file))
        else:
            raise ImageDecodeException(
                'Unsupported bit count %d: %r' % (bitcount, filename or file))

        pitch = (pitch + 3) & ~3
        packed_width = pitch // ctypes.sizeof(bits_type)

        if bitcount < 16 and compression == BI_RGB: 
            clr_used = info_header.biClrUsed or (1 <<  bitcount)
            palette = to_ctypes(buffer, palette_offset, RGBQUAD * clr_used)
            bits = to_ctypes(buffer, bits_offset, 
                             bits_type * packed_width * height)
            return decoder(bits, palette, width, height, pitch, pitch_sign)
        elif bitcount >= 16 and compression == BI_RGB:
            bits = to_ctypes(buffer, bits_offset, 
                             bits_type * (packed_width * height))
            return decoder(bits, None, width, height, pitch, pitch_sign)
        elif compression == BI_BITFIELDS:
            if info_header.biSize >= ctypes.sizeof(BITMAPV4HEADER):
                info_header = to_ctypes(buffer, info_header_offset,
                                        BITMAPV4HEADER)
                r_mask = info_header.bV4RedMask
                g_mask = info_header.bV4GreenMask
                b_mask = info_header.bV4BlueMask
            else:
                fields_offset = info_header_offset + \
                    ctypes.sizeof(BITMAPINFOHEADER) 
                fields = to_ctypes(buffer, fields_offset, RGBFields)
                r_mask = fields.red
                g_mask = fields.green
                b_mask = fields.blue
            class _BitsArray(ctypes.LittleEndianStructure):
                _pack_ = 1
                _fields_ = [
                    ('data', bits_type * packed_width * height),
                ]
            bits = to_ctypes(buffer, bits_offset, _BitsArray).data
            return decoder(bits, r_mask, g_mask, b_mask, 
                           width, height, pitch, pitch_sign)

def decode_1bit(bits, palette, width, height, pitch, pitch_sign):
    rgb_pitch = (((pitch << 3) + 7) & ~0x7) * 3
    buffer = (ctypes.c_ubyte * (height * rgb_pitch))()
    i = 0
    for row in bits:
        for packed in row:
            for _ in range(8):
                rgb = palette[(packed & 0x80) >> 7]
                buffer[i] = rgb.rgbRed
                buffer[i + 1] = rgb.rgbGreen
                buffer[i + 2] = rgb.rgbBlue
                i += 3
                packed <<= 1

    return ImageData(width, height, 'RGB', buffer, pitch_sign * rgb_pitch)

def decode_4bit(bits, palette, width, height, pitch, pitch_sign):
    rgb_pitch = (((pitch << 1) + 1) & ~0x1) * 3
    buffer = (ctypes.c_ubyte * (height * rgb_pitch))()
    i = 0
    for row in bits:
        for packed in row:
            for index in ((packed & 0xf0) >> 4, packed & 0xf):
                rgb = palette[index]
                buffer[i] = rgb.rgbRed
                buffer[i + 1] = rgb.rgbGreen
                buffer[i + 2] = rgb.rgbBlue
                i += 3

    return ImageData(width, height, 'RGB', buffer, pitch_sign * rgb_pitch)

def decode_8bit(bits, palette, width, height, pitch, pitch_sign):
    rgb_pitch = pitch * 3
    buffer = (ctypes.c_ubyte * (height * rgb_pitch))()
    i = 0
    for row in bits:
        for index in row:
            rgb = palette[index]
            buffer[i] = rgb.rgbRed
            buffer[i + 1] = rgb.rgbGreen
            buffer[i + 2] = rgb.rgbBlue
            i += 3

    return ImageData(width, height, 'RGB', buffer, pitch_sign * rgb_pitch)


def decode_24bit(bits, palette, width, height, pitch, pitch_sign):
    buffer = (ctypes.c_ubyte * (height * pitch))()
    ctypes.memmove(buffer, bits, len(buffer))
    return ImageData(width, height, 'BGR', buffer, pitch_sign * pitch)

def decode_32bit_rgb(bits, palette, width, height, pitch, pitch_sign):
    buffer = (ctypes.c_ubyte * (height * pitch))()
    ctypes.memmove(buffer, bits, len(buffer))
    return ImageData(width, height, 'BGRA', buffer, pitch_sign * pitch)

def get_shift(mask):
    if not mask:
        return 0

    # Shift down
    shift = 0
    while not (1 << shift) & mask:
        shift += 1

    # Shift up
    shift_up = 0
    while (mask >> shift) >> shift_up:
        shift_up += 1

    s = shift - (8 - shift_up)
    if s < 0:
        return 0, -s
    else:
        return s, 0

def decode_bitfields(bits, r_mask, g_mask, b_mask, 
                     width, height, pitch, pitch_sign):
    r_shift1, r_shift2 = get_shift(r_mask)
    g_shift1, g_shift2 = get_shift(g_mask)
    b_shift1, b_shift2 = get_shift(b_mask)

    rgb_pitch = 3 * len(bits[0])
    buffer = (ctypes.c_ubyte * (height * rgb_pitch))()

    i = 0
    for row in bits:
        for packed in row:
            buffer[i] = (packed & r_mask) >> r_shift1 << r_shift2
            buffer[i+1] = (packed & g_mask) >> g_shift1 << g_shift2
            buffer[i+2] = (packed & b_mask) >> b_shift1 << b_shift2
            i += 3

    return ImageData(width, height, 'RGB', buffer, pitch_sign * rgb_pitch)

def get_decoders():
    return [BMPImageDecoder()]

def get_encoders():
    return []
