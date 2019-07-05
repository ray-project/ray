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
# $Id: $

"""Text formatting, layout and display.

This module provides classes for loading styled documents from text files,
HTML files and a pyglet-specific markup format.  Documents can be styled with
multiple fonts, colours, styles, text sizes, margins, paragraph alignments,
and so on.  

Using the layout classes, documents can be laid out on a single line or
word-wrapped to fit a rectangle.  A layout can then be efficiently drawn in
a window or updated incrementally (for example, to support interactive text
editing).

The label classes provide a simple interface for the common case where an
application simply needs to display some text in a window.

A plain text label can be created with::

    label = pyglet.text.Label('Hello, world', 
                              font_name='Times New Roman', 
                              font_size=36,
                              x=10, y=10)

Alternatively, a styled text label using HTML can be created with::

    label = pyglet.text.HTMLLabel('<b>Hello</b>, <i>world</i>',
                                  x=10, y=10)

Either label can then be drawn at any time with::

    label.draw()

For details on the subset of HTML supported, see `pyglet.text.formats.html`.

Refer to the Programming Guide for advanced usage of the document and layout
classes, including interactive editing, embedding objects within documents and
creating scrollable layouts.

.. versionadded:: 1.1
"""
from builtins import object

__docformat__ = 'restructuredtext'
__version__ = '$Id: $'

import os.path

import pyglet
from pyglet.text import layout, document, caret


class DocumentDecodeException(Exception):
    """An error occurred decoding document text."""
    pass


class DocumentDecoder(object):
    """Abstract document decoder.
    """

    def decode(self, text, location=None):
        """Decode document text.
        
        :Parameters:
            `text` : str
                Text to decode
            `location` : `Location`
                Location to use as base path for additional resources
                referenced within the document (for example, HTML images).

        :rtype: `AbstractDocument`
        """
        raise NotImplementedError('abstract')


def get_decoder(filename, mimetype=None):
    """Get a document decoder for the given filename and MIME type.

    If `mimetype` is omitted it is guessed from the filename extension.

    The following MIME types are supported:

    ``text/plain``
        Plain text
    ``text/html``
        HTML 4 Transitional
    ``text/vnd.pyglet-attributed``
        Attributed text; see `pyglet.text.formats.attributed`

    `DocumentDecodeException` is raised if another MIME type is given.

    :Parameters:
        `filename` : str
            Filename to guess the MIME type from.  If a MIME type is given,
            the filename is ignored.
        `mimetype` : str
            MIME type to lookup, or ``None`` to guess the type from the
            filename.

    :rtype: `DocumentDecoder`
    """
    if mimetype is None:
        _, ext = os.path.splitext(filename)
        if ext.lower() in ('.htm', '.html', '.xhtml'):
            mimetype = 'text/html'
        else:
            mimetype = 'text/plain'

    if mimetype == 'text/plain':
        from pyglet.text.formats import plaintext
        return plaintext.PlainTextDecoder()
    elif mimetype == 'text/html':
        from pyglet.text.formats import html
        return html.HTMLDecoder()
    elif mimetype == 'text/vnd.pyglet-attributed':
        from pyglet.text.formats import attributed
        return attributed.AttributedTextDecoder()
    else:
        raise DocumentDecodeException('Unknown format "%s"' % mimetype)


def load(filename, file=None, mimetype=None):
    """Load a document from a file.

    :Parameters:
        `filename` : str
            Filename of document to load.
        `file` : file-like object
            File object containing encoded data.  If omitted, `filename` is
            loaded from disk.
        `mimetype` : str
            MIME type of the document.  If omitted, the filename extension is
            used to guess a MIME type.  See `get_decoder` for a list of
            supported MIME types.

    :rtype: `AbstractDocument`
    """
    decoder = get_decoder(filename, mimetype)
    if not file:
        with open(filename) as f:
            file_contents = f.read()
    else:
        file_contents = file.read()
        file.close()

    if hasattr(file_contents, "decode"):
        file_contents = file_contents.decode()

    location = pyglet.resource.FileLocation(os.path.dirname(filename))
    return decoder.decode(file_contents, location)


def decode_html(text, location=None):
    """Create a document directly from some HTML formatted text.

    :Parameters:
        `text` : str
            HTML data to decode.
        `location` : str
            Location giving the base path for additional resources
            referenced from the document (e.g., images).

    :rtype: `FormattedDocument`
    """
    decoder = get_decoder(None, 'text/html')
    return decoder.decode(text, location)


def decode_attributed(text):
    """Create a document directly from some attributed text.

    See `pyglet.text.formats.attributed` for a description of attributed text.

    :Parameters:
        `text` : str
            Attributed text to decode.

    :rtype: `FormattedDocument`
    """
    decoder = get_decoder(None, 'text/vnd.pyglet-attributed')
    return decoder.decode(text)


def decode_text(text):
    """Create a document directly from some plain text.

    :Parameters:
        `text` : str
            Plain text to initialise the document with.

    :rtype: `UnformattedDocument`
    """
    decoder = get_decoder(None, 'text/plain')
    return decoder.decode(text)


class DocumentLabel(layout.TextLayout):
    """Base label class.

    A label is a layout that exposes convenience methods for manipulating the
    associated document.
    """

    def __init__(self, document=None,
                 x=0, y=0, width=None, height=None,
                 anchor_x='left', anchor_y='baseline',
                 multiline=False, dpi=None, batch=None, group=None):
        """Create a label for a given document.

        :Parameters:
            `document` : `AbstractDocument`
                Document to attach to the layout.
            `x` : int
                X coordinate of the label.
            `y` : int
                Y coordinate of the label.
            `width` : int
                Width of the label in pixels, or None
            `height` : int
                Height of the label in pixels, or None
            `anchor_x` : str
                Anchor point of the X coordinate: one of ``"left"``,
                ``"center"`` or ``"right"``.
            `anchor_y` : str
                Anchor point of the Y coordinate: one of ``"bottom"``,
                ``"baseline"``, ``"center"`` or ``"top"``.
            `multiline` : bool
                If True, the label will be word-wrapped and accept newline
                characters.  You must also set the width of the label.
            `dpi` : float
                Resolution of the fonts in this layout.  Defaults to 96.
            `batch` : `~pyglet.graphics.Batch`
                Optional graphics batch to add the label to.
            `group` : `~pyglet.graphics.Group`
                Optional graphics group to use.

        """
        super(DocumentLabel, self).__init__(document,
                                            width=width, height=height,
                                            multiline=multiline,
                                            dpi=dpi, batch=batch, group=group)

        self._x = x
        self._y = y
        self._anchor_x = anchor_x
        self._anchor_y = anchor_y
        self._update()

    @property
    def text(self):
        """The text of the label.

        :type: str
        """
        return self.document.text

    @text.setter
    def text(self, text):
        self.document.text = text

    @property
    def color(self):
        """Text color.

        Color is a 4-tuple of RGBA components, each in range [0, 255].

        :type: (int, int, int, int)
        """
        return self.document.get_style('color')

    @color.setter
    def color(self, color):
        self.document.set_style(0, len(self.document.text),
                                {'color': color})

    @property
    def font_name(self):
        """Font family name.

        The font name, as passed to :py:func:`pyglet.font.load`.  A list of names can
        optionally be given: the first matching font will be used.

        :type: str or list
        """
        return self.document.get_style('font_name')

    @font_name.setter
    def font_name(self, font_name):
        self.document.set_style(0, len(self.document.text),
                                {'font_name': font_name})

    @property
    def font_size(self):
        """Font size, in points.

        :type: float
        """
        return self.document.get_style('font_size')

    @font_size.setter
    def font_size(self, font_size):
        self.document.set_style(0, len(self.document.text),
                                {'font_size': font_size})

    @property
    def bold(self):
        """Bold font style.

        :type: bool
        """
        return self.document.get_style('bold')

    @bold.setter
    def bold(self, bold):
        self.document.set_style(0, len(self.document.text),
                                {'bold': bold})

    @property
    def italic(self):
        """Italic font style.

        :type: bool
        """
        return self.document.get_style('italic')

    @italic.setter
    def italic(self, italic):
        self.document.set_style(0, len(self.document.text),
                                {'italic': italic})

    def get_style(self, name):
        """Get a document style value by name.

        If the document has more than one value of the named style,
        `pyglet.text.document.STYLE_INDETERMINATE` is returned.

        :Parameters:
            `name` : str
                Style name to query.  See documentation for
                `pyglet.text.layout` for known style names.

        :rtype: object
        """
        return self.document.get_style_range(name, 0, len(self.document.text))

    def set_style(self, name, value):
        """Set a document style value by name over the whole document.

        :Parameters:
            `name` : str
                Name of the style to set.  See documentation for
                `pyglet.text.layout` for known style names.
            `value` : object
                Value of the style.

        """
        self.document.set_style(0, len(self.document.text), {name: value})


class Label(DocumentLabel):
    """Plain text label.
    """

    def __init__(self, text='',
                 font_name=None, font_size=None, bold=False, italic=False,
                 color=(255, 255, 255, 255),
                 x=0, y=0, width=None, height=None,
                 anchor_x='left', anchor_y='baseline',
                 align='left',
                 multiline=False, dpi=None, batch=None, group=None):
        """Create a plain text label.

        :Parameters:
            `text` : str
                Text to display.
            `font_name` : str or list
                Font family name(s).  If more than one name is given, the
                first matching name is used.
            `font_size` : float
                Font size, in points.
            `bold` : bool
                Bold font style.
            `italic` : bool
                Italic font style.
            `color` : (int, int, int, int)
                Font colour, as RGBA components in range [0, 255].
            `x` : int
                X coordinate of the label.
            `y` : int
                Y coordinate of the label.
            `width` : int
                Width of the label in pixels, or None
            `height` : int
                Height of the label in pixels, or None
            `anchor_x` : str
                Anchor point of the X coordinate: one of ``"left"``,
                ``"center"`` or ``"right"``.
            `anchor_y` : str
                Anchor point of the Y coordinate: one of ``"bottom"``,
                ``"baseline"``, ``"center"`` or ``"top"``.
            `align` : str
                Horizontal alignment of text on a line, only applies if
                a width is supplied. One of ``"left"``, ``"center"``
                or ``"right"``.
            `multiline` : bool
                If True, the label will be word-wrapped and accept newline
                characters.  You must also set the width of the label.
            `dpi` : float
                Resolution of the fonts in this layout.  Defaults to 96.
            `batch` : `~pyglet.graphics.Batch`
                Optional graphics batch to add the label to.
            `group` : `~pyglet.graphics.Group`
                Optional graphics group to use.

        """
        document = decode_text(text)
        super(Label, self).__init__(document, x, y, width, height,
                                    anchor_x, anchor_y,
                                    multiline, dpi, batch, group)

        self.document.set_style(0, len(self.document.text), {
            'font_name': font_name,
            'font_size': font_size,
            'bold': bold,
            'italic': italic,
            'color': color,
            'align': align,
        })


class HTMLLabel(DocumentLabel):
    """HTML formatted text label.
    
    A subset of HTML 4.01 is supported.  See `pyglet.text.formats.html` for
    details.
    """

    def __init__(self, text='', location=None,
                 x=0, y=0, width=None, height=None,
                 anchor_x='left', anchor_y='baseline',
                 multiline=False, dpi=None, batch=None, group=None):
        """Create a label with an HTML string.

        :Parameters:
            `text` : str
                HTML formatted text to display.
            `location` : `Location`
                Location object for loading images referred to in the
                document.  By default, the working directory is used.
            `x` : int
                X coordinate of the label.
            `y` : int
                Y coordinate of the label.
            `width` : int
                Width of the label in pixels, or None
            `height` : int
                Height of the label in pixels, or None
            `anchor_x` : str
                Anchor point of the X coordinate: one of ``"left"``,
                ``"center"`` or ``"right"``.
            `anchor_y` : str
                Anchor point of the Y coordinate: one of ``"bottom"``,
                ``"baseline"``, ``"center"`` or ``"top"``.
            `multiline` : bool
                If True, the label will be word-wrapped and render paragraph
                and line breaks.  You must also set the width of the label.
            `dpi` : float
                Resolution of the fonts in this layout.  Defaults to 96.
            `batch` : `~pyglet.graphics.Batch`
                Optional graphics batch to add the label to.
            `group` : `~pyglet.graphics.Group`
                Optional graphics group to use.

        """
        self._text = text
        self._location = location
        document = decode_html(text, location)
        super(HTMLLabel, self).__init__(document, x, y, width, height,
                                        anchor_x, anchor_y,
                                        multiline, dpi, batch, group)

    @property
    def text(self):
        """HTML formatted text of the label.

        :type: str
        """
        return self._text

    @text.setter
    def text(self, text):
        self._text = text
        self.document = decode_html(text, self._location)
