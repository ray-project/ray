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
from __future__ import division
from builtins import object

import ctypes
from collections import namedtuple

from pyglet.compat import asbytes, asstr
from pyglet.font import base
from pyglet import image
from pyglet.font.fontconfig import get_fontconfig
from pyglet.font.freetype_lib import *


class FreeTypeGlyphRenderer(base.GlyphRenderer):
    def __init__(self, font):
        super(FreeTypeGlyphRenderer, self).__init__(font)
        self.font = font

        self._glyph_slot = None
        self._bitmap = None

        self._width = None
        self._height = None
        self._mode = None
        self._pitch = None

        self._baseline = None
        self._lsb = None
        self._advance_x = None

        self._data = None

    def _get_glyph(self, character):
        assert self.font
        assert len(character) == 1

        self._glyph_slot = self.font.get_glyph_slot(character)
        self._bitmap = self._glyph_slot.bitmap

    def _get_glyph_metrics(self):
        self._width = self._glyph_slot.bitmap.width
        self._height = self._glyph_slot.bitmap.rows
        self._mode = self._glyph_slot.bitmap.pixel_mode
        self._pitch = self._glyph_slot.bitmap.pitch

        self._baseline = self._height - self._glyph_slot.bitmap_top
        self._lsb = self._glyph_slot.bitmap_left
        self._advance_x = int(f26p6_to_float(self._glyph_slot.advance.x))

    def _get_bitmap_data(self):
        if self._mode == FT_PIXEL_MODE_MONO:
            # BCF fonts always render to 1 bit mono, regardless of render
            # flags. (freetype 2.3.5)
            self._convert_mono_to_gray_bitmap()
        elif self._mode == FT_PIXEL_MODE_GRAY:
            # Usual case
            assert self._glyph_slot.bitmap.num_grays == 256
            self._data = self._glyph_slot.bitmap.buffer
        else:
            raise base.FontException('Unsupported render mode for this glyph')

    def _convert_mono_to_gray_bitmap(self):
        bitmap_data = cast(self._bitmap.buffer,
                           POINTER(c_ubyte * (self._pitch * self._height))).contents
        data = (c_ubyte * (self._pitch * 8 * self._height))()
        data_i = 0
        for byte in bitmap_data:
            # Data is MSB; left-most pixel in a byte has value 128.
            data[data_i + 0] = (byte & 0x80) and 255 or 0
            data[data_i + 1] = (byte & 0x40) and 255 or 0
            data[data_i + 2] = (byte & 0x20) and 255 or 0
            data[data_i + 3] = (byte & 0x10) and 255 or 0
            data[data_i + 4] = (byte & 0x08) and 255 or 0
            data[data_i + 5] = (byte & 0x04) and 255 or 0
            data[data_i + 6] = (byte & 0x02) and 255 or 0
            data[data_i + 7] = (byte & 0x01) and 255 or 0
            data_i += 8
        self._data = data
        self._pitch <<= 3

    def _create_glyph(self):
        # In FT positive pitch means `down` flow, in Pyglet ImageData
        # negative values indicate a top-to-bottom arrangement. So pitch must be inverted.
        # Using negative pitch causes conversions, so much faster to just swap tex_coords
        img = image.ImageData(self._width,
                              self._height,
                              'A',
                              self._data,
                              abs(self._pitch))
        glyph = self.font.create_glyph(img)
        glyph.set_bearings(self._baseline, self._lsb, self._advance_x)
        if self._pitch > 0:
            t = list(glyph.tex_coords)
            glyph.tex_coords = t[9:12] + t[6:9] + t[3:6] + t[:3]

        return glyph

    def render(self, text):
        self._get_glyph(text[0])
        self._get_glyph_metrics()
        self._get_bitmap_data()
        return self._create_glyph()


FreeTypeFontMetrics = namedtuple('FreeTypeFontMetrics',
                                 ['ascent', 'descent'])


class MemoryFaceStore(object):
    def __init__(self):
        self._dict = {}

    def add(self, face):
        self._dict[face.name.lower(), face.bold, face.italic] = face

    def contains(self, name):
        lname = name and name.lower() or ''
        return len([name for name, _, _ in self._dict.keys() if name == lname]) > 0

    def get(self, name, bold, italic):
        lname = name and name.lower() or ''
        return self._dict.get((lname, bold, italic), None)


class FreeTypeFont(base.Font):
    glyph_renderer_class = FreeTypeGlyphRenderer

    # Map font (name, bold, italic) to FreeTypeMemoryFace
    _memory_faces = MemoryFaceStore()

    def __init__(self, name, size, bold=False, italic=False, dpi=None):
        super(FreeTypeFont, self).__init__()

        self.name = name
        self.size = size
        self.bold = bold
        self.italic = italic
        self.dpi = dpi or 96  # as of pyglet 1.1; pyglet 1.0 had 72.

        self._load_font_face()
        self.metrics = self.face.get_font_metrics(self.size, self.dpi)

    @property
    def ascent(self):
        return self.metrics.ascent

    @property
    def descent(self):
        return self.metrics.descent

    def get_glyph_slot(self, character):
        glyph_index = self.face.get_character_index(character)
        self.face.set_char_size(self.size, self.dpi)
        return self.face.get_glyph_slot(glyph_index)

    def _load_font_face(self):
        self.face = self._memory_faces.get(self.name, self.bold, self.italic)
        if self.face is None:
            self._load_font_face_from_system()

    def _load_font_face_from_system(self):
        match = get_fontconfig().find_font(self.name, self.size, self.bold, self.italic)
        if not match:
            raise base.FontException('Could not match font "%s"' % self.name)
        self.face = FreeTypeFace.from_fontconfig(match)

    @classmethod
    def have_font(cls, name):
        if cls._memory_faces.contains(name):
            return True
        else:
            return get_fontconfig().have_font(name)

    @classmethod
    def add_font_data(cls, data):
        face = FreeTypeMemoryFace(data)
        cls._memory_faces.add(face)


class FreeTypeFace(object):
    """FreeType typographic face object.

    Keeps the reference count to the face at +1 as long as this object exists. If other objects
    want to keep a face without a reference to this object, they should increase the reference
    counter themselves and decrease it again when done.
    """
    def __init__(self, ft_face):
        assert ft_face is not None
        self.ft_face = ft_face
        self._get_best_name()

    @classmethod
    def from_file(cls, file_name):
        ft_library = ft_get_library()
        ft_face = FT_Face()
        FT_New_Face(ft_library,
                    asbytes(file_name),
                    0,
                    byref(ft_face))
        return cls(ft_face)

    @classmethod
    def from_fontconfig(cls, match):
        if match.face is not None:
            FT_Reference_Face(match.face)
            return cls(match.face)
        else:
            if not match.file:
                raise base.FontException('No filename for "%s"' % match.name)
            return cls.from_file(match.file)

    @property
    def family_name(self):
        return asstr(self.ft_face.contents.family_name)

    @property
    def style_flags(self):
        return self.ft_face.contents.style_flags

    @property
    def bold(self):
        return self.style_flags & FT_STYLE_FLAG_BOLD != 0

    @property
    def italic(self):
        return self.style_flags & FT_STYLE_FLAG_ITALIC != 0

    @property
    def face_flags(self):
        return self.ft_face.contents.face_flags

    def __del__(self):
        if self.ft_face is not None:
            FT_Done_Face(self.ft_face)
            self.ft_face = None

    def set_char_size(self, size, dpi):
        face_size = float_to_f26p6(size)
        try:
            FT_Set_Char_Size(self.ft_face,
                             0,
                             face_size,
                             dpi,
                             dpi)
            return True
        except FreeTypeError as e:
            # Error 0x17 indicates invalid pixel size, so font size cannot be changed
            # TODO Warn the user?
            if e.errcode == 0x17:
                return False
            else:
                raise

    def get_character_index(self, character):
        return get_fontconfig().char_index(self.ft_face, character)

    def get_glyph_slot(self, glyph_index):
        FT_Load_Glyph(self.ft_face, glyph_index, FT_LOAD_RENDER)
        return self.ft_face.contents.glyph.contents

    def get_font_metrics(self, size, dpi):
        if self.set_char_size(size, dpi):
            metrics = self.ft_face.contents.size.contents.metrics
            if metrics.ascender == 0 and metrics.descender == 0:
                return self._get_font_metrics_workaround()
            else:
                return FreeTypeFontMetrics(ascent=int(f26p6_to_float(metrics.ascender)),
                                           descent=int(f26p6_to_float(metrics.descender)))
        else:
            return self._get_font_metrics_workaround()

    def _get_font_metrics_workaround(self):
        # Workaround broken fonts with no metrics.  Has been observed with
        # courR12-ISO8859-1.pcf.gz: "Courier" "Regular"
        #
        # None of the metrics fields are filled in, so render a glyph and
        # grab its height as the ascent, and make up an arbitrary
        # descent.
        i = self.get_character_index('X')
        self.get_glyph_slot(i)
        ascent=self.ft_face.contents.available_sizes.contents.height
        return FreeTypeFontMetrics(ascent=ascent,
                                   descent=-ascent // 4)  # arbitrary.

    def _get_best_name(self):
        self.name = self.family_name
        self._get_font_family_from_ttf

    def _get_font_family_from_ttf(self):
        # Replace Freetype's generic family name with TTF/OpenType specific
        # name if we can find one; there are some instances where Freetype
        # gets it wrong.

        return  # FIXME: This is broken

        if self.face_flags & FT_FACE_FLAG_SFNT:
            name = FT_SfntName()
            for i in range(FT_Get_Sfnt_Name_Count(self.ft_face)):
                try:
                    FT_Get_Sfnt_Name(self.ft_face, i, name)
                    if not (name.platform_id == TT_PLATFORM_MICROSOFT and
                            name.encoding_id == TT_MS_ID_UNICODE_CS):
                        continue
                    # name.string is not 0 terminated! use name.string_len
                    self.name = name.string.decode('utf-16be', 'ignore')
                except:
                    continue


class FreeTypeMemoryFace(FreeTypeFace):
    def __init__(self, data):
        self._copy_font_data(data)
        super(FreeTypeMemoryFace, self).__init__(self._create_font_face())

    def _copy_font_data(self, data):
        self.font_data = (FT_Byte * len(data))()
        ctypes.memmove(self.font_data, data, len(data))

    def _create_font_face(self):
        ft_library = ft_get_library()
        ft_face = FT_Face()
        FT_New_Memory_Face(ft_library,
                           self.font_data,
                           len(self.font_data),
                           0,
                           byref(ft_face))
        return ft_face

