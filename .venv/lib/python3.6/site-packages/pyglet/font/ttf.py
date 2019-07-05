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
# $Id$

"""
Implementation of the Truetype file format.

Typical applications will not need to use this module directly; look at
`pyglyph.font` instead.  

References:
 * http://developer.apple.com/fonts/TTRefMan/RM06
 * http://www.microsoft.com/typography/otspec
"""
from __future__ import division
from builtins import zip
from builtins import chr
from builtins import range
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id$'  

import codecs
import os
import mmap
import struct

class TruetypeInfo(object):
    """Information about a single Truetype face.

    The class memory-maps the font file to read the tables, so
    it is vital that you call the `close` method to avoid large memory
    leaks.  Once closed, you cannot call any of the ``get_*`` methods.

    Not all tables have been implemented yet (or likely ever will).
    Currently only the name and metric tables are read; in particular
    there is no glyph or hinting information.
    """
    
    _name_id_lookup = {
        'copyright': 0,
        'family': 1,
        'subfamily': 2,
        'identifier': 3,
        'name': 4,
        'version': 5,
        'postscript': 6,
        'trademark': 7,
        'manufacturer': 8,
        'designer': 9,
        'description': 10,
        'vendor-url': 11,
        'designer-url': 12,
        'license': 13,
        'license-url': 14,
        'preferred-family': 16,
        'preferred-subfamily': 17,
        'compatible-name': 18,
        'sample': 19,
    }

    _platform_id_lookup = {
        'unicode': 0,
        'macintosh': 1,
        'iso': 2,
        'microsoft': 3,
        'custom': 4
    }

    _microsoft_encoding_lookup = {
        1: 'utf_16_be',
        2: 'shift_jis',
        4: 'big5',
        6: 'johab',
        10: 'utf_16_be'
    }

    _macintosh_encoding_lookup = {
        0: 'mac_roman'
    }

    def __init__(self, filename):
        """Read the given TrueType file.

        :Parameters:
            `filename`
                The name of any Windows, OS2 or Macintosh Truetype file.

        The object must be closed (see `close`) after use.

        An exception will be raised if the file does not exist or cannot
        be read.
        """
        if not filename: filename = ''
        len = os.stat(filename).st_size
        self._fileno = os.open(filename, os.O_RDONLY)
        if hasattr(mmap, 'MAP_SHARED'):
            self._data = mmap.mmap(self._fileno, len, mmap.MAP_SHARED,
                mmap.PROT_READ)
        else:
            self._data = mmap.mmap(self._fileno, len, None, mmap.ACCESS_READ)

        offsets = _read_offset_table(self._data, 0)
        self._tables = {}
        for table in _read_table_directory_entry.array(self._data, 
            offsets.size, offsets.num_tables):
            self._tables[table.tag] = table

        self._names = None
        self._horizontal_metrics = None
        self._character_advances = None
        self._character_kernings = None
        self._glyph_kernings = None
        self._character_map = None
        self._glyph_map = None
        self._font_selection_flags = None

        self.header = \
            _read_head_table(self._data, self._tables['head'].offset)
        self.horizontal_header = \
            _read_horizontal_header(self._data, self._tables['hhea'].offset)

    def get_font_selection_flags(self):
        """Return the font selection flags, as defined in OS/2 table"""
        if not self._font_selection_flags:
            OS2_table = \
                _read_OS2_table(self._data, self._tables['OS/2'].offset)
            self._font_selection_flags = OS2_table.fs_selection
        return self._font_selection_flags

    def is_bold(self):
        """Returns True iff the font describes itself as bold."""
        return bool(self.get_font_selection_flags() & 0x20)

    def is_italic(self):
        """Returns True iff the font describes itself as italic."""
        return bool(self.get_font_selection_flags() & 0x1)

    def get_names(self):
        """Returns a dictionary of names defined in the file.

        The key of each item is a tuple of ``platform_id``, ``name_id``,
        where each ID is the number as described in the Truetype format.

        The value of each item is a tuple of 
        ``encoding_id``, ``language_id``, ``value``, where ``value`` is
        an encoded string.
        """
        if self._names:
            return self._names
        naming_table = \
            _read_naming_table(self._data, self._tables['name'].offset)
        name_records = \
            _read_name_record.array(self._data,
                self._tables['name'].offset + naming_table.size,
                naming_table.count)
        storage = naming_table.string_offset + self._tables['name'].offset
        self._names = {}
        for record in name_records:
            value = self._data[record.offset + storage:\
                               record.offset + storage + record.length]
            key = record.platform_id, record.name_id
            value = (record.encoding_id, record.language_id, value)
            if not key in self._names:
                self._names[key] = []
            self._names[key].append(value)
        return self._names

    def get_name(self, name, platform=None, languages=None):
        """Returns the value of the given name in this font.

        :Parameters:
            `name`
                Either an integer, representing the name_id desired (see
                font format); or a string describing it, see below for 
                valid names.
            `platform`
                Platform for the requested name.  Can be the integer ID,
                or a string describing it.  By default, the Microsoft
                platform is searched first, then Macintosh.
            `languages`
                A list of language IDs to search.  The first language
                which defines the requested name will be used.  By default,
                all English dialects are searched.

        If the name is not found, ``None`` is returned.  If the name
        is found, the value will be decoded and returned as a unicode
        string.  Currently only some common encodings are supported.

        Valid names to request are (supply as a string)::

            'copyright'
            'family'
            'subfamily'
            'identifier'
            'name'
            'version'
            'postscript'
            'trademark'
            'manufacturer'
            'designer'
            'description'
            'vendor-url'
            'designer-url'
            'license'
            'license-url'
            'preferred-family'
            'preferred-subfamily'
            'compatible-name'
            'sample'

        Valid platforms to request are (supply as a string)::

            'unicode'
            'macintosh'
            'iso'
            'microsoft'
            'custom'
        """
        
        names = self.get_names()
        if type(name) == str:
            name = self._name_id_lookup[name]
        if not platform:
            for platform in ('microsoft','macintosh'):
                value = self.get_name(name, platform, languages)
                if value:
                    return value
        if type(platform) == str:
            platform = self._platform_id_lookup[platform]
        if not (platform, name) in names:
            return None

        if platform == 3: # setup for microsoft
            encodings = self._microsoft_encoding_lookup
            if not languages:
                # Default to english languages for microsoft
                languages = (0x409,0x809,0xc09,0x1009,0x1409,0x1809)
        elif platform == 1: # setup for macintosh
            encodings = self.__macintosh_encoding_lookup
            if not languages:
                # Default to english for macintosh
                languages = (0,)
            
        for record in names[(platform, name)]:
            if record[1] in languages and record[0] in encodings:
                decoder = codecs.getdecoder(encodings[record[0]])
                return decoder(record[2])[0]
        return None                

    def get_horizontal_metrics(self):
        """Return all horizontal metric entries in table format."""
        if not self._horizontal_metrics:
            ar = _read_long_hor_metric.array(self._data, 
                 self._tables['hmtx'].offset,
                 self.horizontal_header.number_of_h_metrics)
            self._horizontal_metrics = ar
        return self._horizontal_metrics

    def get_character_advances(self):
        """Return a dictionary of character->advance.

        They key of the dictionary is a unit-length unicode string,
        and the value is a float giving the horizontal advance in
        em.
        """
        if self._character_advances:
            return self._character_advances
        ga = self.get_glyph_advances()
        gmap = self.get_glyph_map()
        self._character_advances = {}
        for i in range(len(ga)):
            if i in gmap and not gmap[i] in self._character_advances:
                self._character_advances[gmap[i]] = ga[i]
        return self._character_advances

    def get_glyph_advances(self):
        """Return a dictionary of glyph->advance.

        They key of the dictionary is the glyph index and the value is a float
        giving the horizontal advance in em.
        """
        hm = self.get_horizontal_metrics()
        return [float(m.advance_width) / self.header.units_per_em for m in hm]

    def get_character_kernings(self):
        """Return a dictionary of (left,right)->kerning

        The key of the dictionary is a tuple of ``(left, right)``
        where each element is a unit-length unicode string.  The
        value of the dictionary is the horizontal pairwise kerning
        in em.
        """
        if not self._character_kernings:
            gmap = self.get_glyph_map()
            kerns = self.get_glyph_kernings()
            self._character_kernings = {}
            for pair, value in kerns.items():
                lglyph, rglyph = pair
                lchar = lglyph in gmap and gmap[lglyph] or None
                rchar = rglyph in gmap and gmap[rglyph] or None
                if lchar and rchar:
                    self._character_kernings[(lchar, rchar)] = value
        return self._character_kernings

    def get_glyph_kernings(self):
        """Return a dictionary of (left,right)->kerning

        The key of the dictionary is a tuple of ``(left, right)``
        where each element is a glyph index.  The value of the dictionary is
        the horizontal pairwise kerning in em.
        """
        if self._glyph_kernings:
            return self._glyph_kernings
        header = \
            _read_kern_header_table(self._data, self._tables['kern'].offset)
        offset = self._tables['kern'].offset + header.size
        kernings = {}
        for i in range(header.n_tables):
            header = _read_kern_subtable_header(self._data, offset)
            if header.coverage & header.horizontal_mask \
               and not header.coverage & header.minimum_mask \
               and not header.coverage & header.perpendicular_mask:
                if header.coverage & header.format_mask == 0:
                    self._add_kernings_format0(kernings, offset + header.size)
            offset += header.length
        self._glyph_kernings = kernings
        return kernings
                
    def _add_kernings_format0(self, kernings, offset):
        header = _read_kern_subtable_format0(self._data, offset)
        kerning_pairs = _read_kern_subtable_format0Pair.array(self._data, 
            offset + header.size, header.n_pairs)
        for pair in kerning_pairs:
            if (pair.left, pair.right) in kernings:
                kernings[(pair.left, pair.right)] += pair.value \
                    / float(self.header.units_per_em)
            else:
                kernings[(pair.left, pair.right)] = pair.value \
                    / float(self.header.units_per_em)

    def get_glyph_map(self):
        """Calculate and return a reverse character map.

        Returns a dictionary where the key is a glyph index and the
        value is a unit-length unicode string.
        """
        if self._glyph_map:
            return self._glyph_map
        cmap = self.get_character_map()
        self._glyph_map = {}
        for ch, glyph in cmap.items():
            if not glyph in self._glyph_map:
                self._glyph_map[glyph] = ch
        return self._glyph_map

    def get_character_map(self):
        """Return the character map.

        Returns a dictionary where the key is a unit-length unicode
        string and the value is a glyph index.  Currently only 
        format 4 character maps are read.
        """
        if self._character_map:
            return self._character_map
        cmap = _read_cmap_header(self._data, self._tables['cmap'].offset)
        records = _read_cmap_encoding_record.array(self._data,
            self._tables['cmap'].offset + cmap.size, cmap.num_tables)
        self._character_map = {}
        for record in records:
            if record.platform_id == 3 and record.encoding_id == 1:
                # Look at Windows Unicode charmaps only
                offset = self._tables['cmap'].offset + record.offset
                format_header = _read_cmap_format_header(self._data, offset)
                if format_header.format == 4:
                    self._character_map = \
                        self._get_character_map_format4(offset)
                    break
        return self._character_map

    def _get_character_map_format4(self, offset):
        # This is absolutely, without question, the *worst* file
        # format ever.  Whoever the fuckwit is that thought this up is
        # a fuckwit. 
        header = _read_cmap_format4Header(self._data, offset)
        seg_count = header.seg_count_x2 // 2
        array_size = struct.calcsize('>%dH' % seg_count)
        end_count = self._read_array('>%dH' % seg_count, 
            offset + header.size)
        start_count = self._read_array('>%dH' % seg_count, 
            offset + header.size + array_size + 2)
        id_delta = self._read_array('>%dh' % seg_count,
            offset + header.size + array_size + 2 + array_size)
        id_range_offset_address = \
            offset + header.size + array_size + 2 + array_size + array_size
        id_range_offset = self._read_array('>%dH' % seg_count, 
            id_range_offset_address)
        character_map = {}
        for i in range(0, seg_count):
            if id_range_offset[i] != 0:
                if id_range_offset[i] == 65535:
                    continue  # Hack around a dodgy font (babelfish.ttf)
                for c in range(start_count[i], end_count[i] + 1):
                    addr = id_range_offset[i] + 2*(c - start_count[i]) + \
                        id_range_offset_address + 2*i
                    g = struct.unpack('>H', self._data[addr:addr+2])[0]
                    if g != 0:
                        character_map[chr(c)] = (g + id_delta[i]) % 65536
            else:
                for c in range(start_count[i], end_count[i] + 1):
                    g = (c + id_delta[i]) % 65536
                    if g != 0:
                        character_map[chr(c)] = g
        return character_map

    def _read_array(self, format, offset):
        size = struct.calcsize(format)
        return struct.unpack(format, self._data[offset:offset+size])

    def close(self):
        """Close the font file.

        This is a good idea, since the entire file is memory mapped in
        until this method is called.  After closing cannot rely on the
        ``get_*`` methods.
        """
        
        self._data.close()
        os.close(self._fileno)

def _read_table(*entries):
    """ Generic table constructor used for table formats listed at
    end of file."""
    fmt = '>'
    names = []
    for entry in entries:
        name, type = entry.split(':')
        names.append(name)
        fmt += type
    class _table_class(object):
        size = struct.calcsize(fmt)
        def __init__(self, data, offset):
            items = struct.unpack(fmt, data[offset:offset+self.size])
            self.pairs = list(zip(names, items))
            for name, value in self.pairs:
                setattr(self, name, value)

        def __repr__(self):
            s = '{' +  ', '.join(['%s = %s' % (name, value) \
                                  for name, value in self.pairs]) + '}'
            return s

        @staticmethod
        def array(data, offset, count):
            tables = []
            for i in range(count):
                tables.append(_table_class(data, offset))
                offset += _table_class.size
            return tables

    return _table_class


# Table formats (see references)

_read_offset_table = _read_table('scalertype:I',
                        'num_tables:H',
                        'search_range:H',
                        'entry_selector:H',
                        'range_shift:H')

_read_table_directory_entry = _read_table('tag:4s',
                                'check_sum:I',
                                'offset:I',
                                'length:I')
_read_head_table = _read_table('version:i',
                          'font_revision:i',
                          'check_sum_adjustment:L',
                          'magic_number:L',
                          'flags:H',
                          'units_per_em:H',
                          'created:Q',
                          'modified:Q',
                          'x_min:h',
                          'y_min:h',
                          'x_max:h',
                          'y_max:h',
                          'mac_style:H',
                          'lowest_rec_p_pEM:H',
                          'font_direction_hint:h',
                          'index_to_loc_format:h',
                          'glyph_data_format:h')

_read_OS2_table = _read_table('version:H',
                         'x_avg_char_width:h',
                         'us_weight_class:H',
                         'us_width_class:H',
                         'fs_type:H',
                         'y_subscript_x_size:h',
                         'y_subscript_y_size:h',
                         'y_subscript_x_offset:h',
                         'y_subscript_y_offset:h',
                         'y_superscript_x_size:h',
                         'y_superscript_y_size:h',
                         'y_superscript_x_offset:h',
                         'y_superscript_y_offset:h',
                         'y_strikeout_size:h',
                         'y_strikeout_position:h',
                         's_family_class:h',
                         'panose1:B',
                         'panose2:B',
                         'panose3:B',
                         'panose4:B',
                         'panose5:B',
                         'panose6:B',
                         'panose7:B',
                         'panose8:B',
                         'panose9:B',
                         'panose10:B',
                         'ul_unicode_range1:L',
                         'ul_unicode_range2:L',
                         'ul_unicode_range3:L',
                         'ul_unicode_range4:L',
                         'ach_vend_id:I',
                         'fs_selection:H',
                         'us_first_char_index:H',
                         'us_last_char_index:H',
                         's_typo_ascender:h',
                         's_typo_descender:h',
                         's_typo_line_gap:h',
                         'us_win_ascent:H',
                         'us_win_descent:H',
                         'ul_code_page_range1:L',
                         'ul_code_page_range2:L',
                         'sx_height:h',
                         's_cap_height:h',
                         'us_default_char:H',
                         'us_break_char:H',
                         'us_max_context:H')

_read_kern_header_table = _read_table('version_num:H',
                                'n_tables:H')

_read_kern_subtable_header = _read_table('version:H',
                                   'length:H',
                                   'coverage:H')
_read_kern_subtable_header.horizontal_mask = 0x1
_read_kern_subtable_header.minimum_mask = 0x2
_read_kern_subtable_header.perpendicular_mask = 0x4
_read_kern_subtable_header.override_mask = 0x5
_read_kern_subtable_header.format_mask = 0xf0

_read_kern_subtable_format0 = _read_table('n_pairs:H',
                                    'search_range:H',
                                    'entry_selector:H',
                                    'range_shift:H')
_read_kern_subtable_format0Pair = _read_table('left:H',
                                        'right:H',
                                        'value:h')

_read_cmap_header = _read_table('version:H',
                           'num_tables:H')

_read_cmap_encoding_record = _read_table('platform_id:H',
                                   'encoding_id:H',
                                   'offset:L')

_read_cmap_format_header = _read_table('format:H',
                                 'length:H')
_read_cmap_format4Header = _read_table('format:H',
                                  'length:H',
                                  'language:H',
                                  'seg_count_x2:H',
                                  'search_range:H',
                                  'entry_selector:H',
                                  'range_shift:H')

_read_horizontal_header = _read_table('version:i',
                                 'Advance:h',
                                 'Descender:h',
                                 'LineGap:h',
                                 'advance_width_max:H',
                                 'min_left_side_bearing:h',
                                 'min_right_side_bearing:h',
                                 'x_max_extent:h',
                                 'caret_slope_rise:h',
                                 'caret_slope_run:h',
                                 'caret_offset:h',
                                 'reserved1:h',
                                 'reserved2:h',
                                 'reserved3:h',
                                 'reserved4:h',
                                 'metric_data_format:h',
                                 'number_of_h_metrics:H')

_read_long_hor_metric = _read_table('advance_width:H',
                              'lsb:h')

_read_naming_table = _read_table('format:H',
                            'count:H',
                            'string_offset:H')

_read_name_record = _read_table('platform_id:H',
                           'encoding_id:H',
                           'language_id:H',
                           'name_id:H',
                           'length:H',
                           'offset:H')
