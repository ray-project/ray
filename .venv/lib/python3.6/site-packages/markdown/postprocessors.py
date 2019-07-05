# -*- coding: utf-8 -*-
"""
Python Markdown

A Python implementation of John Gruber's Markdown.

Documentation: https://python-markdown.github.io/
GitHub: https://github.com/Python-Markdown/markdown/
PyPI: https://pypi.org/project/Markdown/

Started by Manfred Stienstra (http://www.dwerg.net/).
Maintained for a few years by Yuri Takhteyev (http://www.freewisdom.org).
Currently maintained by Waylan Limberg (https://github.com/waylan),
Dmitry Shachnev (https://github.com/mitya57) and Isaac Muse (https://github.com/facelessuser).

Copyright 2007-2018 The Python Markdown Project (v. 1.7 and later)
Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
Copyright 2004 Manfred Stienstra (the original version)

License: BSD (see LICENSE.md for details).

POST-PROCESSORS
=============================================================================

Markdown also allows post-processors, which are similar to preprocessors in
that they need to implement a "run" method. However, they are run after core
processing.

"""

from __future__ import absolute_import
from __future__ import unicode_literals
from collections import OrderedDict
from . import util
import re


def build_postprocessors(md, **kwargs):
    """ Build the default postprocessors for Markdown. """
    postprocessors = util.Registry()
    postprocessors.register(RawHtmlPostprocessor(md), 'raw_html', 30)
    postprocessors.register(AndSubstitutePostprocessor(), 'amp_substitute', 20)
    postprocessors.register(UnescapePostprocessor(), 'unescape', 10)
    return postprocessors


class Postprocessor(util.Processor):
    """
    Postprocessors are run after the ElementTree it converted back into text.

    Each Postprocessor implements a "run" method that takes a pointer to a
    text string, modifies it as necessary and returns a text string.

    Postprocessors must extend markdown.Postprocessor.

    """

    def run(self, text):
        """
        Subclasses of Postprocessor should implement a `run` method, which
        takes the html document as a single text string and returns a
        (possibly modified) string.

        """
        pass  # pragma: no cover


class RawHtmlPostprocessor(Postprocessor):
    """ Restore raw html to the document. """

    def run(self, text):
        """ Iterate over html stash and restore html. """
        replacements = OrderedDict()
        for i in range(self.md.htmlStash.html_counter):
            html = self.md.htmlStash.rawHtmlBlocks[i]
            if self.isblocklevel(html):
                replacements["<p>%s</p>" %
                             (self.md.htmlStash.get_placeholder(i))] = \
                    html + "\n"
            replacements[self.md.htmlStash.get_placeholder(i)] = html

        if replacements:
            pattern = re.compile("|".join(re.escape(k) for k in replacements))
            processed_text = pattern.sub(lambda m: replacements[m.group(0)], text)
        else:
            return text

        if processed_text == text:
            return processed_text
        else:
            return self.run(processed_text)

    def isblocklevel(self, html):
        m = re.match(r'^\<\/?([^ >]+)', html)
        if m:
            if m.group(1)[0] in ('!', '?', '@', '%'):
                # Comment, php etc...
                return True
            return self.md.is_block_level(m.group(1))
        return False


class AndSubstitutePostprocessor(Postprocessor):
    """ Restore valid entities """

    def run(self, text):
        text = text.replace(util.AMP_SUBSTITUTE, "&")
        return text


class UnescapePostprocessor(Postprocessor):
    """ Restore escaped chars """

    RE = re.compile(r'%s(\d+)%s' % (util.STX, util.ETX))

    def unescape(self, m):
        return util.int2str(int(m.group(1)))

    def run(self, text):
        return self.RE.sub(self.unescape, text)
