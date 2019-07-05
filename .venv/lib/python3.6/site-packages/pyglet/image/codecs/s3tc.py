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
# $Id:$

'''Software decoder for S3TC compressed texture (i.e., DDS).

http://oss.sgi.com/projects/ogl-sample/registry/EXT/texture_compression_s3tc.txt
'''
from __future__ import division
from builtins import range

import ctypes
import re

from pyglet.gl import *
from pyglet.gl import gl_info
from pyglet.image import AbstractImage, Texture

split_8byte = re.compile('.' * 8, flags=re.DOTALL)
split_16byte = re.compile('.' * 16, flags=re.DOTALL)

class PackedImageData(AbstractImage):
    _current_texture = None

    def __init__(self, width, height, format, packed_format, data):
        super(PackedImageData, self).__init__(width, height)
        self.format = format
        self.packed_format = packed_format
        self.data = data

    def unpack(self):
        if self.packed_format == GL_UNSIGNED_SHORT_5_6_5:
            # Unpack to GL_RGB.  Assume self.data is already 16-bit
            i = 0
            out = (ctypes.c_ubyte * (self.width * self.height * 3))()
            for c in self.data:
                out[i+2] = (c & 0x1f) << 3
                out[i+1] = (c & 0x7e0) >> 3
                out[i] = (c & 0xf800) >> 8
                i += 3
            self.data = out
            self.packed_format = GL_UNSIGNED_BYTE

    def _get_texture(self):
        if self._current_texture:
            return self._current_texture

        texture = Texture.create_for_size(
            GL_TEXTURE_2D, self.width, self.height)
        glBindTexture(texture.target, texture.id)
        glTexParameteri(texture.target, GL_TEXTURE_MIN_FILTER, GL_LINEAR)

        if not gl_info.have_version(1, 2) or True:
            self.unpack()

        glTexImage2D(texture.target, texture.level,
            self.format, self.width, self.height, 0,
            self.format, self.packed_format, self.data)

        self._current_texture = texture
        return texture
    
    texture = property(_get_texture)

    def get_texture(self, rectangle=False, force_rectangle=False):
        '''The parameters 'rectangle' and 'force_rectangle' are ignored.
           See the documentation of the method 'AbstractImage.get_texture' for
           a more detailed documentation of the method. '''
        return self._get_texture()

def decode_dxt1_rgb(data, width, height):
    # Decode to 16-bit RGB UNSIGNED_SHORT_5_6_5
    out = (ctypes.c_uint16 * (width * height))()

    # Read 8 bytes at a time
    image_offset = 0
    for c0_lo, c0_hi, c1_lo, c1_hi, b0, b1, b2, b3 in split_8byte.findall(data):
        color0 = ord(c0_lo) | ord(c0_hi) << 8
        color1 = ord(c1_lo) | ord(c1_hi) << 8
        bits = ord(b0) | ord(b1) << 8 | ord(b2) << 16 | ord(b3) << 24

        r0 = color0 & 0x1f
        g0 = (color0 & 0x7e0) >> 5
        b0 = (color0 & 0xf800) >> 11
        r1 = color1 & 0x1f
        g1 = (color1 & 0x7e0) >> 5
        b1 = (color1 & 0xf800) >> 11

        # i is the dest ptr for this block
        i = image_offset
        for y in range(4):
            for x in range(4):
                code = bits & 0x3

                if code == 0:
                    out[i] = color0
                elif code == 1:
                    out[i] = color1
                elif code == 3 and color0 <= color1:
                    out[i] = 0
                else:
                    if code == 2 and color0 > color1:
                        r = (2 * r0 + r1) // 3
                        g = (2 * g0 + g1) // 3
                        b = (2 * b0 + b1) // 3
                    elif code == 3 and color0 > color1:
                        r = (r0 + 2 * r1) // 3
                        g = (g0 + 2 * g1) // 3
                        b = (b0 + 2 * b1) // 3
                    else:
                        assert code == 2 and color0 <= color1
                        r = (r0 + r1) // 2
                        g = (g0 + g1) // 2
                        b = (b0 + b1) // 2
                    out[i] = r | g << 5 | b << 11

                bits >>= 2
                i += 1
            i += width - 4

        # Move dest ptr to next 4x4 block
        advance_row = (image_offset + 4) % width == 0
        image_offset += width * 3 * advance_row + 4

    return PackedImageData(width, height, 
        GL_RGB, GL_UNSIGNED_SHORT_5_6_5, out)

def decode_dxt1_rgba(data, width, height):
    # Decode to GL_RGBA
    out = (ctypes.c_ubyte * (width * height * 4))()
    pitch = width << 2

    # Read 8 bytes at a time
    image_offset = 0
    for c0_lo, c0_hi, c1_lo, c1_hi, b0, b1, b2, b3 in split_8byte.findall(data):
        color0 = ord(c0_lo) | ord(c0_hi) << 8
        color1 = ord(c1_lo) | ord(c1_hi) << 8
        bits = ord(b0) | ord(b1) << 8 | ord(b2) << 16 | ord(b3) << 24

        r0 = color0 & 0x1f
        g0 = (color0 & 0x7e0) >> 5
        b0 = (color0 & 0xf800) >> 11
        r1 = color1 & 0x1f
        g1 = (color1 & 0x7e0) >> 5
        b1 = (color1 & 0xf800) >> 11

        # i is the dest ptr for this block
        i = image_offset
        for y in range(4):
            for x in range(4):
                code = bits & 0x3
                a = 255

                if code == 0:
                    r, g, b = r0, g0, b0
                elif code == 1:
                    r, g, b = r1, g1, b1
                elif code == 3 and color0 <= color1:
                    r = g = b = a = 0
                else:
                    if code == 2 and color0 > color1:
                        r = (2 * r0 + r1) // 3
                        g = (2 * g0 + g1) // 3
                        b = (2 * b0 + b1) // 3
                    elif code == 3 and color0 > color1:
                        r = (r0 + 2 * r1) // 3
                        g = (g0 + 2 * g1) // 3
                        b = (b0 + 2 * b1) // 3
                    else:
                        assert code == 2 and color0 <= color1
                        r = (r0 + r1) // 2
                        g = (g0 + g1) // 2
                        b = (b0 + b1) // 2

                out[i] = b << 3
                out[i+1] = g << 2
                out[i+2] = r << 3
                out[i+3] = a << 4

                bits >>= 2
                i += 4
            i += pitch - 16

        # Move dest ptr to next 4x4 block
        advance_row = (image_offset + 16) % pitch == 0
        image_offset += pitch * 3 * advance_row + 16

    return PackedImageData(width, height, GL_RGBA, GL_UNSIGNED_BYTE, out)


def decode_dxt3(data, width, height):
    # Decode to GL_RGBA
    out = (ctypes.c_ubyte * (width * height * 4))()
    pitch = width << 2

    # Read 16 bytes at a time
    image_offset = 0
    for (a0, a1, a2, a3, a4, a5, a6, a7,
         c0_lo, c0_hi, c1_lo, c1_hi, 
         b0, b1, b2, b3) in split_16byte.findall(data):
        color0 = ord(c0_lo) | ord(c0_hi) << 8
        color1 = ord(c1_lo) | ord(c1_hi) << 8
        bits = ord(b0) | ord(b1) << 8 | ord(b2) << 16 | ord(b3) << 24
        alpha = ord(a0) | ord(a1) << 8 | ord(a2) << 16 | ord(a3) << 24 | \
            ord(a4) << 32 | ord(a5) << 40 | ord(a6) << 48 | ord(a7) << 56

        r0 = color0 & 0x1f
        g0 = (color0 & 0x7e0) >> 5
        b0 = (color0 & 0xf800) >> 11
        r1 = color1 & 0x1f
        g1 = (color1 & 0x7e0) >> 5
        b1 = (color1 & 0xf800) >> 11

        # i is the dest ptr for this block
        i = image_offset
        for y in range(4):
            for x in range(4):
                code = bits & 0x3
                a = alpha & 0xf

                if code == 0:
                    r, g, b = r0, g0, b0
                elif code == 1:
                    r, g, b = r1, g1, b1
                elif code == 3 and color0 <= color1:
                    r = g = b = 0
                else:
                    if code == 2 and color0 > color1:
                        r = (2 * r0 + r1) // 3
                        g = (2 * g0 + g1) // 3
                        b = (2 * b0 + b1) // 3
                    elif code == 3 and color0 > color1:
                        r = (r0 + 2 * r1) // 3
                        g = (g0 + 2 * g1) // 3
                        b = (b0 + 2 * b1) // 3
                    else:
                        assert code == 2 and color0 <= color1
                        r = (r0 + r1) // 2
                        g = (g0 + g1) // 2
                        b = (b0 + b1) // 2

                out[i] = b << 3
                out[i+1] = g << 2
                out[i+2] = r << 3
                out[i+3] = a << 4

                bits >>= 2
                alpha >>= 4
                i += 4
            i += pitch - 16

        # Move dest ptr to next 4x4 block
        advance_row = (image_offset + 16) % pitch == 0
        image_offset += pitch * 3 * advance_row + 16

    return PackedImageData(width, height, GL_RGBA, GL_UNSIGNED_BYTE, out)

def decode_dxt5(data, width, height):
    # Decode to GL_RGBA
    out = (ctypes.c_ubyte * (width * height * 4))()
    pitch = width << 2

    # Read 16 bytes at a time
    image_offset = 0
    for (alpha0, alpha1, ab0, ab1, ab2, ab3, ab4, ab5, 
         c0_lo, c0_hi, c1_lo, c1_hi, 
         b0, b1, b2, b3) in split_16byte.findall(data):
        color0 = ord(c0_lo) | ord(c0_hi) << 8
        color1 = ord(c1_lo) | ord(c1_hi) << 8
        alpha0 = ord(alpha0)
        alpha1 = ord(alpha1)
        bits = ord(b0) | ord(b1) << 8 | ord(b2) << 16 | ord(b3) << 24
        abits = ord(ab0) | ord(ab1) << 8 | ord(ab2) << 16 | ord(ab3) << 24 | \
            ord(ab4) << 32 | ord(ab5) << 40

        r0 = color0 & 0x1f
        g0 = (color0 & 0x7e0) >> 5
        b0 = (color0 & 0xf800) >> 11
        r1 = color1 & 0x1f
        g1 = (color1 & 0x7e0) >> 5
        b1 = (color1 & 0xf800) >> 11

        # i is the dest ptr for this block
        i = image_offset
        for y in range(4):
            for x in range(4):
                code = bits & 0x3
                acode = abits & 0x7

                if code == 0:
                    r, g, b = r0, g0, b0
                elif code == 1:
                    r, g, b = r1, g1, b1
                elif code == 3 and color0 <= color1:
                    r = g = b = 0
                else:
                    if code == 2 and color0 > color1:
                        r = (2 * r0 + r1) // 3
                        g = (2 * g0 + g1) // 3
                        b = (2 * b0 + b1) // 3
                    elif code == 3 and color0 > color1:
                        r = (r0 + 2 * r1) // 3
                        g = (g0 + 2 * g1) // 3
                        b = (b0 + 2 * b1) // 3
                    else:
                        assert code == 2 and color0 <= color1
                        r = (r0 + r1) / 2
                        g = (g0 + g1) / 2
                        b = (b0 + b1) / 2
                
                if acode == 0:
                    a = alpha0
                elif acode == 1:
                    a = alpha1
                elif alpha0 > alpha1:
                    if acode == 2:
                        a = (6 * alpha0 + 1 * alpha1) // 7
                    elif acode == 3:
                        a = (5 * alpha0 + 2 * alpha1) // 7
                    elif acode == 4:
                        a = (4 * alpha0 + 3 * alpha1) // 7
                    elif acode == 5:
                        a = (3 * alpha0 + 4 * alpha1) // 7
                    elif acode == 6:
                        a = (2 * alpha0 + 5 * alpha1) // 7
                    else:
                        assert acode == 7
                        a = (1 * alpha0 + 6 * alpha1) // 7
                else:
                    if acode == 2:
                        a = (4 * alpha0 + 1 * alpha1) // 5
                    elif acode == 3:
                        a = (3 * alpha0 + 2 * alpha1) // 5
                    elif acode == 4:
                        a = (2 * alpha0 + 3 * alpha1) // 5
                    elif acode == 5:
                        a = (1 * alpha0 + 4 * alpha1) // 5
                    elif acode == 6:
                        a = 0
                    else:
                        assert acode == 7
                        a = 255

                out[i] = b << 3
                out[i+1] = g << 2
                out[i+2] = r << 3
                out[i+3] = a

                bits >>= 2
                abits >>= 3
                i += 4
            i += pitch - 16

        # Move dest ptr to next 4x4 block
        advance_row = (image_offset + 16) % pitch == 0
        image_offset += pitch * 3 * advance_row + 16

    return PackedImageData(width, height, GL_RGBA, GL_UNSIGNED_BYTE, out)
