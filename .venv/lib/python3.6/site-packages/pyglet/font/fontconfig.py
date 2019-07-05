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
Wrapper around the Linux FontConfig library. Used to find available fonts.
"""
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id$'

from collections import OrderedDict
from ctypes import *

import pyglet.lib
from pyglet.compat import asbytes, asstr
from pyglet.font.base import FontException

# fontconfig library definitions

(FcResultMatch,
 FcResultNoMatch,
 FcResultTypeMismatch,
 FcResultNoId,
 FcResultOutOfMemory) = range(5)
FcResult = c_int

FC_FAMILY = asbytes('family')
FC_SIZE = asbytes('size')
FC_SLANT = asbytes('slant')
FC_WEIGHT = asbytes('weight')
FC_FT_FACE = asbytes('ftface')
FC_FILE = asbytes('file')

FC_WEIGHT_REGULAR = 80
FC_WEIGHT_BOLD = 200

FC_SLANT_ROMAN = 0
FC_SLANT_ITALIC = 100

(FcTypeVoid,
 FcTypeInteger,
 FcTypeDouble,
 FcTypeString,
 FcTypeBool,
 FcTypeMatrix,
 FcTypeCharSet,
 FcTypeFTFace,
 FcTypeLangSet) = range(9)
FcType = c_int

(FcMatchPattern,
 FcMatchFont) = range(2)
FcMatchKind = c_int


class _FcValueUnion(Union):
    _fields_ = [
        ('s', c_char_p),
        ('i', c_int),
        ('b', c_int),
        ('d', c_double),
        ('m', c_void_p),
        ('c', c_void_p),
        ('f', c_void_p),
        ('p', c_void_p),
        ('l', c_void_p),
    ]


class FcValue(Structure):
    _fields_ = [
        ('type', FcType),
        ('u', _FcValueUnion)
    ]

# End of library definitions


class FontConfig(object):
    def __init__(self):
        self._fontconfig = self._load_fontconfig_library()
        self._search_cache = OrderedDict()
        self._cache_size = 20

    def dispose(self):
        while len(self._search_cache) > 0:
            self._search_cache.popitem().dispose()

        self._fontconfig.FcFini()
        self._fontconfig = None

    def create_search_pattern(self):
        return FontConfigSearchPattern(self._fontconfig)

    def find_font(self, name, size=12, bold=False, italic=False):
        result = self._get_from_search_cache(name, size, bold, italic)
        if result:
            return result

        search_pattern = self.create_search_pattern()
        search_pattern.name = name
        search_pattern.size = size
        search_pattern.bold = bold
        search_pattern.italic = italic

        result = search_pattern.match()
        self._add_to_search_cache(search_pattern, result)
        search_pattern.dispose()
        return result

    def have_font(self, name):
        result = self.find_font(name)
        if result:
            # Check the name matches, fontconfig can return a default
            if name and result.name and result.name.lower() != name.lower():
                return False
            return True
        else:
            return False

    def char_index(self, ft_face, character):
        return self._fontconfig.FcFreeTypeCharIndex(ft_face, ord(character))

    def _add_to_search_cache(self, search_pattern, result_pattern):
        self._search_cache[(search_pattern.name,
                            search_pattern.size,
                            search_pattern.bold,
                            search_pattern.italic)] = result_pattern
        if len(self._search_cache) > self._cache_size:
            self._search_cache.popitem(last=False)[1].dispose()

    def _get_from_search_cache(self,  name, size, bold, italic):
        result = self._search_cache.get((name, size, bold, italic), None)

        if result and result.is_valid:
            return result
        else:
            return None

    @staticmethod
    def _load_fontconfig_library():
        fontconfig = pyglet.lib.load_library('fontconfig')
        fontconfig.FcInit()

        fontconfig.FcPatternBuild.restype = c_void_p
        fontconfig.FcPatternCreate.restype = c_void_p
        fontconfig.FcFontMatch.restype = c_void_p
        fontconfig.FcFreeTypeCharIndex.restype = c_uint

        fontconfig.FcPatternAddDouble.argtypes = [c_void_p, c_char_p, c_double]
        fontconfig.FcPatternAddInteger.argtypes = [c_void_p, c_char_p, c_int]
        fontconfig.FcPatternAddString.argtypes = [c_void_p, c_char_p, c_char_p]
        fontconfig.FcConfigSubstitute.argtypes = [c_void_p, c_void_p, c_int]
        fontconfig.FcDefaultSubstitute.argtypes = [c_void_p]
        fontconfig.FcFontMatch.argtypes = [c_void_p, c_void_p, c_void_p]
        fontconfig.FcPatternDestroy.argtypes = [c_void_p]

        fontconfig.FcPatternGetFTFace.argtypes = [c_void_p, c_char_p, c_int, c_void_p]
        fontconfig.FcPatternGet.argtypes = [c_void_p, c_char_p, c_int, c_void_p]

        return fontconfig


class FontConfigPattern(object):
    def __init__(self, fontconfig, pattern=None):
        self._fontconfig = fontconfig
        self._pattern = pattern

    @property
    def is_valid(self):
        return self._fontconfig and self._pattern

    def _create(self):
        assert not self._pattern
        assert self._fontconfig
        self._pattern = self._fontconfig.FcPatternCreate()

    def _destroy(self):
        assert self._pattern
        assert self._fontconfig
        self._fontconfig.FcPatternDestroy(self._pattern)
        self._pattern = None

    @staticmethod
    def _bold_to_weight(bold):
        return FC_WEIGHT_BOLD if bold else FC_WEIGHT_REGULAR

    @staticmethod
    def _italic_to_slant(italic):
        return FC_SLANT_ITALIC if italic else FC_SLANT_ROMAN

    def _set_string(self, name, value):
        assert self._pattern
        assert name
        assert self._fontconfig

        if not value:
            return

        value = value.encode('utf8')

        self._fontconfig.FcPatternAddString(self._pattern, name, asbytes(value))

    def _set_double(self, name, value):
        assert self._pattern
        assert name
        assert self._fontconfig

        if not value:
            return

        self._fontconfig.FcPatternAddDouble(self._pattern, name, c_double(value))

    def _set_integer(self, name, value):
        assert self._pattern
        assert name
        assert self._fontconfig

        if not value:
            return

        self._fontconfig.FcPatternAddInteger(self._pattern, name, c_int(value))

    def _get_value(self, name):
        assert self._pattern
        assert name
        assert self._fontconfig

        value = FcValue()
        result = self._fontconfig.FcPatternGet(self._pattern, name, 0, byref(value))
        if _handle_fcresult(result):
            return value
        else:
            return None

    def _get_string(self, name):
        value = self._get_value(name)

        if value and value.type == FcTypeString:
            return asstr(value.u.s)
        else:
            return None

    def _get_face(self, name):
        value = self._get_value(name)

        if value and value.type == FcTypeFTFace:
            return value.u.f
        else:
            return None

    def _get_integer(self, name):
        value = self._get_value(name)

        if value and value.type == FcTypeInteger:
            return value.u.i
        else:
            return None

    def _get_double(self, name):
        value = self._get_value(name)

        if value and value.type == FcTypeDouble:
            return value.u.d
        else:
            return None


class FontConfigSearchPattern(FontConfigPattern):
    def __init__(self, fontconfig):
        super(FontConfigSearchPattern, self).__init__(fontconfig)

        self.name = None
        self.bold = False
        self.italic = False
        self.size = None

    def match(self):
        self._prepare_search_pattern()
        result_pattern = self._get_match()

        if result_pattern:
            return FontConfigSearchResult(self._fontconfig, result_pattern)
        else:
            return None

    def _prepare_search_pattern(self):
        self._create()
        self._set_string(FC_FAMILY, self.name)
        self._set_double(FC_SIZE, self.size)
        self._set_integer(FC_WEIGHT, self._bold_to_weight(self.bold))
        self._set_integer(FC_SLANT, self._italic_to_slant(self.italic))

        self._substitute_defaults()

    def _substitute_defaults(self):
        assert self._pattern
        assert self._fontconfig

        self._fontconfig.FcConfigSubstitute(None, self._pattern, FcMatchPattern)
        self._fontconfig.FcDefaultSubstitute(self._pattern)

    def _get_match(self):
        assert self._pattern
        assert self._fontconfig

        match_result = FcResult()
        match_pattern = self._fontconfig.FcFontMatch(0, self._pattern, byref(match_result))

        if _handle_fcresult(match_result.value):
            return match_pattern
        else:
            return None

    def dispose(self):
        self._destroy()


class FontConfigSearchResult(FontConfigPattern):
    def __init__(self, fontconfig, result_pattern):
        super(FontConfigSearchResult, self).__init__(fontconfig, result_pattern)

    @property
    def name(self):
        return self._get_string(FC_FAMILY)

    @property
    def size(self):
        return self._get_double(FC_SIZE)

    @property
    def bold(self):
        return self._get_integer(FC_WEIGHT) == FC_WEIGHT_BOLD

    @property
    def italic(self):
        return self._get_integer(FC_SLANT) == FC_SLANT_ITALIC

    @property
    def face(self):
        return self._get_face(FC_FT_FACE)

    @property
    def file(self):
        return self._get_string(FC_FILE)

    def dispose(self):
        self._destroy()


def _handle_fcresult(result):
    if result == FcResultMatch:
        return True
    elif result in (FcResultNoMatch, FcResultTypeMismatch, FcResultNoId):
        return False
    elif result == FcResultOutOfMemory:
        raise FontException('FontConfig ran out of memory.')


_fontconfig_instance = None


def get_fontconfig():
    global _fontconfig_instance
    if not _fontconfig_instance:
        _fontconfig_instance = FontConfig()
    return _fontconfig_instance
