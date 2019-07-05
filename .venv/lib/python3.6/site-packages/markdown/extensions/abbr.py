'''
Abbreviation Extension for Python-Markdown
==========================================

This extension adds abbreviation handling to Python-Markdown.

See <https://Python-Markdown.github.io/extensions/abbreviations>
for documentation.

Oringinal code Copyright 2007-2008 [Waylan Limberg](http://achinghead.com/) and
 [Seemant Kulleen](http://www.kulleen.org/)

All changes Copyright 2008-2014 The Python Markdown Project

License: [BSD](http://www.opensource.org/licenses/bsd-license.php)

'''

from __future__ import absolute_import
from __future__ import unicode_literals
from . import Extension
from ..preprocessors import Preprocessor
from ..inlinepatterns import InlineProcessor
from ..util import etree, AtomicString
import re

# Global Vars
ABBR_REF_RE = re.compile(r'[*]\[(?P<abbr>[^\]]*)\][ ]?:\s*(?P<title>.*)')


class AbbrExtension(Extension):
    """ Abbreviation Extension for Python-Markdown. """

    def extendMarkdown(self, md):
        """ Insert AbbrPreprocessor before ReferencePreprocessor. """
        md.preprocessors.register(AbbrPreprocessor(md), 'abbr', 12)


class AbbrPreprocessor(Preprocessor):
    """ Abbreviation Preprocessor - parse text for abbr references. """

    def run(self, lines):
        '''
        Find and remove all Abbreviation references from the text.
        Each reference is set as a new AbbrPattern in the markdown instance.

        '''
        new_text = []
        for line in lines:
            m = ABBR_REF_RE.match(line)
            if m:
                abbr = m.group('abbr').strip()
                title = m.group('title').strip()
                self.md.inlinePatterns.register(
                    AbbrInlineProcessor(self._generate_pattern(abbr), title), 'abbr-%s' % abbr, 2
                )
                # Preserve the line to prevent raw HTML indexing issue.
                # https://github.com/Python-Markdown/markdown/issues/584
                new_text.append('')
            else:
                new_text.append(line)
        return new_text

    def _generate_pattern(self, text):
        '''
        Given a string, returns an regex pattern to match that string.

        'HTML' -> r'(?P<abbr>[H][T][M][L])'

        Note: we force each char as a literal match (in brackets) as we don't
        know what they will be beforehand.

        '''
        chars = list(text)
        for i in range(len(chars)):
            chars[i] = r'[%s]' % chars[i]
        return r'(?P<abbr>\b%s\b)' % (r''.join(chars))


class AbbrInlineProcessor(InlineProcessor):
    """ Abbreviation inline pattern. """

    def __init__(self, pattern, title):
        super(AbbrInlineProcessor, self).__init__(pattern)
        self.title = title

    def handleMatch(self, m, data):
        abbr = etree.Element('abbr')
        abbr.text = AtomicString(m.group('abbr'))
        abbr.set('title', self.title)
        return abbr, m.start(0), m.end(0)


def makeExtension(**kwargs):  # pragma: no cover
    return AbbrExtension(**kwargs)
