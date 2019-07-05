"""
NL2BR Extension
===============

A Python-Markdown extension to treat newlines as hard breaks; like
GitHub-flavored Markdown does.

See <https://Python-Markdown.github.io/extensions/nl2br>
for documentation.

Oringinal code Copyright 2011 [Brian Neal](http://deathofagremmie.com/)

All changes Copyright 2011-2014 The Python Markdown Project

License: [BSD](http://www.opensource.org/licenses/bsd-license.php)

"""

from __future__ import absolute_import
from __future__ import unicode_literals
from . import Extension
from ..inlinepatterns import SubstituteTagInlineProcessor

BR_RE = r'\n'


class Nl2BrExtension(Extension):

    def extendMarkdown(self, md):
        br_tag = SubstituteTagInlineProcessor(BR_RE, 'br')
        md.inlinePatterns.register(br_tag, 'nl', 5)


def makeExtension(**kwargs):  # pragma: no cover
    return Nl2BrExtension(**kwargs)
