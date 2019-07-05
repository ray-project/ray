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

'''Base class for structured (hierarchical) document formats.
'''
from __future__ import division
from builtins import range
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import re

import pyglet

class ImageElement(pyglet.text.document.InlineElement):
    def __init__(self, image, width=None, height=None):
        self.image = image.get_texture()
        self.width = width is None and image.width or width
        self.height = height is None and image.height or height
        self.vertex_lists = {}

        anchor_y = self.height // image.height * image.anchor_y
        ascent = max(0, self.height - anchor_y)
        descent = min(0, -anchor_y)
        super(ImageElement, self).__init__(ascent, descent, self.width)

    def place(self, layout, x, y):
        group = pyglet.graphics.TextureGroup(self.image.texture, 
                                             layout.top_group)
        x1 = x
        y1 = y + self.descent
        x2 = x + self.width
        y2 = y + self.height + self.descent
        vertex_list = layout.batch.add(4, pyglet.gl.GL_QUADS, group,
            ('v2i', (x1, y1, x2, y1, x2, y2, x1, y2)),
            ('c3B', (255, 255, 255) * 4),
            ('t3f', self.image.tex_coords))
        self.vertex_lists[layout] = vertex_list

    def remove(self, layout):
        self.vertex_lists[layout].delete()
        del self.vertex_lists[layout]

def _int_to_roman(input):
    # From http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/81611
    if not 0 < input < 4000:
        raise ValueError("Argument must be between 1 and 3999")    
    ints = (1000, 900,  500, 400, 100,  90, 50,  40, 10,  9,   5,   4,  1)
    nums = ('M',  'CM', 'D', 'CD','C', 'XC','L','XL','X','IX','V','IV','I')
    result = ""
    for i in range(len(ints)):
        count = int(input // ints[i])
        result += nums[i] * count
        input -= ints[i] * count
    return result

class ListBuilder(object):
    def begin(self, decoder, style):
        '''Begin a list.

        :Parameters:
            `decoder` : `StructuredTextDecoder`
                Decoder.
            `style` : dict
                Style dictionary that applies over the entire list.

        '''
        left_margin = decoder.current_style.get('margin_left') or 0
        tab_stops = decoder.current_style.get('tab_stops')
        if tab_stops:
            tab_stops = list(tab_stops)
        else:
            tab_stops = []
        tab_stops.append(left_margin + 50)
        style['margin_left'] = left_margin + 50
        style['indent'] = -30
        style['tab_stops'] = tab_stops

    def item(self, decoder, style, value=None):
        '''Begin a list item.

        :Parameters:
            `decoder` : `StructuredTextDecoder`
                Decoder.
            `style` : dict
                Style dictionary that applies over the list item.
            `value` : str
                Optional value of the list item.  The meaning is list-type
                dependent.

        '''            
        mark = self.get_mark(value)
        if mark:
            decoder.add_text(mark)
        decoder.add_text('\t')

    def get_mark(self, value=None):
        '''Get the mark text for the next list item.

        :Parameters:
            `value` : str
                Optional value of the list item.  The meaning is list-type
                dependent.

        :rtype: str
        '''
        return ''

class UnorderedListBuilder(ListBuilder):
    def __init__(self, mark):
        '''Create an unordered list with constant mark text.

        :Parameters:
            `mark` : str
                Mark to prepend to each list item.

        '''
        self.mark = mark


    def get_mark(self, value):
        return self.mark

class OrderedListBuilder(ListBuilder):
    format_re = re.compile('(.*?)([1aAiI])(.*)')

    def __init__(self, start, format):
        '''Create an ordered list with sequentially numbered mark text.

        The format is composed of an optional prefix text, a numbering
        scheme character followed by suffix text. Valid numbering schemes
        are:

        ``1``
            Decimal Arabic
        ``a``
            Lowercase alphanumeric
        ``A``
            Uppercase alphanumeric
        ``i``
            Lowercase Roman
        ``I``
            Uppercase Roman

        Prefix text may typically be ``(`` or ``[`` and suffix text is
        typically ``.``, ``)`` or empty, but either can be any string.

        :Parameters:
            `start` : int
                First list item number.
            `format` : str
                Format style, for example ``"1."``.

        '''
        self.next_value = start

        self.prefix, self.numbering, self.suffix = self.format_re.match(format).groups()
        assert self.numbering in '1aAiI'

    def get_mark(self, value):
        if value is None:
            value = self.next_value
        self.next_value = value + 1
        if self.numbering in 'aA':
            try:
                mark = 'abcdefghijklmnopqrstuvwxyz'[value - 1]
            except ValueError:
                mark = '?'
            if self.numbering == 'A':
                mark = mark.upper()
            return '%s%s%s' % (self.prefix, mark, self.suffix)
        elif self.numbering in 'iI':
            try:
                mark = _int_to_roman(value)
            except ValueError:
                mark = '?'
            if self.numbering == 'i':
                mark = mark.lower()
            return '%s%s%s' % (self.prefix, mark, self.suffix)
        else:
            return '%s%d%s' % (self.prefix, value, self.suffix)

class StructuredTextDecoder(pyglet.text.DocumentDecoder):
    def decode(self, text, location=None):
        self.len_text = 0
        self.current_style = {}
        self.next_style = {}
        self.stack = []
        self.list_stack = []
        self.document = pyglet.text.document.FormattedDocument()
        if location is None:
            location = pyglet.resource.FileLocation('')
        self.decode_structured(text, location)
        return self.document

    def decode_structured(self, text, location):
        raise NotImplementedError('abstract') 

    def push_style(self, key, styles):
        old_styles = {}
        for name in styles.keys():
            old_styles[name] = self.current_style.get(name)
        self.stack.append((key, old_styles))
        self.current_style.update(styles)
        self.next_style.update(styles)

    def pop_style(self, key):
        # Don't do anything if key is not in stack
        for match, _ in self.stack:
            if key == match:
                break
        else:
            return

        # Remove all innermost elements until key is closed.
        while True:
            match, old_styles = self.stack.pop()
            self.next_style.update(old_styles)
            self.current_style.update(old_styles)
            if match == key:
                break

    def add_text(self, text):
        self.document.insert_text(self.len_text, text, self.next_style)
        self.next_style.clear()
        self.len_text += len(text)

    def add_element(self, element):
        self.document.insert_element(self.len_text, element, self.next_style)
        self.next_style.clear()
        self.len_text += 1
