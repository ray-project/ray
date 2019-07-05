'''
Legacy Em Extension for Python-Markdown
=======================================

This extention provides legacy behavior for _connected_words_.

Copyright 2015-2018 The Python Markdown Project

License: [BSD](http://www.opensource.org/licenses/bsd-license.php)

'''

from __future__ import absolute_import
from __future__ import unicode_literals
from . import Extension
from ..inlinepatterns import SimpleTagInlineProcessor

EMPHASIS_RE = r'(\*|_)(.+?)\1'
STRONG_RE = r'(\*{2}|_{2})(.+?)\1'


class LegacyEmExtension(Extension):
    """ Add legacy_em extension to Markdown class."""

    def extendMarkdown(self, md):
        """ Modify inline patterns. """
        md.inlinePatterns.register(SimpleTagInlineProcessor(STRONG_RE, 'strong'), 'strong', 40)
        md.inlinePatterns.register(SimpleTagInlineProcessor(EMPHASIS_RE, 'em'), 'emphasis', 30)
        md.inlinePatterns.deregister('strong2')
        md.inlinePatterns.deregister('emphasis2')
