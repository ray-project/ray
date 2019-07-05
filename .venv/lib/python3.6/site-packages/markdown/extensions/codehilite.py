"""
CodeHilite Extension for Python-Markdown
========================================

Adds code/syntax highlighting to standard Python-Markdown code blocks.

See <https://Python-Markdown.github.io/extensions/code_hilite>
for documentation.

Original code Copyright 2006-2008 [Waylan Limberg](http://achinghead.com/).

All changes Copyright 2008-2014 The Python Markdown Project

License: [BSD](http://www.opensource.org/licenses/bsd-license.php)

"""

from __future__ import absolute_import
from __future__ import unicode_literals
from . import Extension
from ..treeprocessors import Treeprocessor

try:
    from pygments import highlight
    from pygments.lexers import get_lexer_by_name, guess_lexer
    from pygments.formatters import get_formatter_by_name
    pygments = True
except ImportError:
    pygments = False


def parse_hl_lines(expr):
    """Support our syntax for emphasizing certain lines of code.

    expr should be like '1 2' to emphasize lines 1 and 2 of a code block.
    Returns a list of ints, the line numbers to emphasize.
    """
    if not expr:
        return []

    try:
        return list(map(int, expr.split()))
    except ValueError:
        return []


# ------------------ The Main CodeHilite Class ----------------------
class CodeHilite(object):
    """
    Determine language of source code, and pass it into pygments hilighter.

    Basic Usage:
        >>> code = CodeHilite(src = 'some text')
        >>> html = code.hilite()

    * src: Source string or any object with a .readline attribute.

    * linenums: (Boolean) Set line numbering to 'on' (True),
      'off' (False) or 'auto'(None). Set to 'auto' by default.

    * guess_lang: (Boolean) Turn language auto-detection
      'on' or 'off' (on by default).

    * css_class: Set class name of wrapper div ('codehilite' by default).

    * hl_lines: (List of integers) Lines to emphasize, 1-indexed.

    Low Level Usage:
        >>> code = CodeHilite()
        >>> code.src = 'some text' # String or anything with a .readline attr.
        >>> code.linenos = True  # Turns line numbering on or of.
        >>> html = code.hilite()

    """

    def __init__(self, src=None, linenums=None, guess_lang=True,
                 css_class="codehilite", lang=None, style='default',
                 noclasses=False, tab_length=4, hl_lines=None, use_pygments=True):
        self.src = src
        self.lang = lang
        self.linenums = linenums
        self.guess_lang = guess_lang
        self.css_class = css_class
        self.style = style
        self.noclasses = noclasses
        self.tab_length = tab_length
        self.hl_lines = hl_lines or []
        self.use_pygments = use_pygments

    def hilite(self):
        """
        Pass code to the [Pygments](http://pygments.pocoo.org/) highliter with
        optional line numbers. The output should then be styled with css to
        your liking. No styles are applied by default - only styling hooks
        (i.e.: <span class="k">).

        returns : A string of html.

        """

        self.src = self.src.strip('\n')

        if self.lang is None:
            self._parseHeader()

        if pygments and self.use_pygments:
            try:
                lexer = get_lexer_by_name(self.lang)
            except ValueError:
                try:
                    if self.guess_lang:
                        lexer = guess_lexer(self.src)
                    else:
                        lexer = get_lexer_by_name('text')
                except ValueError:
                    lexer = get_lexer_by_name('text')
            formatter = get_formatter_by_name('html',
                                              linenos=self.linenums,
                                              cssclass=self.css_class,
                                              style=self.style,
                                              noclasses=self.noclasses,
                                              hl_lines=self.hl_lines)
            return highlight(self.src, lexer, formatter)
        else:
            # just escape and build markup usable by JS highlighting libs
            txt = self.src.replace('&', '&amp;')
            txt = txt.replace('<', '&lt;')
            txt = txt.replace('>', '&gt;')
            txt = txt.replace('"', '&quot;')
            classes = []
            if self.lang:
                classes.append('language-%s' % self.lang)
            if self.linenums:
                classes.append('linenums')
            class_str = ''
            if classes:
                class_str = ' class="%s"' % ' '.join(classes)
            return '<pre class="%s"><code%s>%s</code></pre>\n' % \
                   (self.css_class, class_str, txt)

    def _parseHeader(self):
        """
        Determines language of a code block from shebang line and whether said
        line should be removed or left in place. If the sheband line contains a
        path (even a single /) then it is assumed to be a real shebang line and
        left alone. However, if no path is given (e.i.: #!python or :::python)
        then it is assumed to be a mock shebang for language identifitation of
        a code fragment and removed from the code block prior to processing for
        code highlighting. When a mock shebang (e.i: #!python) is found, line
        numbering is turned on. When colons are found in place of a shebang
        (e.i.: :::python), line numbering is left in the current state - off
        by default.

        Also parses optional list of highlight lines, like:

            :::python hl_lines="1 3"
        """

        import re

        # split text into lines
        lines = self.src.split("\n")
        # pull first line to examine
        fl = lines.pop(0)

        c = re.compile(r'''
            (?:(?:^::+)|(?P<shebang>^[#]!)) # Shebang or 2 or more colons
            (?P<path>(?:/\w+)*[/ ])?        # Zero or 1 path
            (?P<lang>[\w#.+-]*)             # The language
            \s*                             # Arbitrary whitespace
            # Optional highlight lines, single- or double-quote-delimited
            (hl_lines=(?P<quot>"|')(?P<hl_lines>.*?)(?P=quot))?
            ''',  re.VERBOSE)
        # search first line for shebang
        m = c.search(fl)
        if m:
            # we have a match
            try:
                self.lang = m.group('lang').lower()
            except IndexError:
                self.lang = None
            if m.group('path'):
                # path exists - restore first line
                lines.insert(0, fl)
            if self.linenums is None and m.group('shebang'):
                # Overridable and Shebang exists - use line numbers
                self.linenums = True

            self.hl_lines = parse_hl_lines(m.group('hl_lines'))
        else:
            # No match
            lines.insert(0, fl)

        self.src = "\n".join(lines).strip("\n")


# ------------------ The Markdown Extension -------------------------------


class HiliteTreeprocessor(Treeprocessor):
    """ Hilight source code in code blocks. """

    def code_unescape(self, text):
        """Unescape code."""
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        return text

    def run(self, root):
        """ Find code blocks and store in htmlStash. """
        blocks = root.iter('pre')
        for block in blocks:
            if len(block) == 1 and block[0].tag == 'code':
                code = CodeHilite(
                    self.code_unescape(block[0].text),
                    linenums=self.config['linenums'],
                    guess_lang=self.config['guess_lang'],
                    css_class=self.config['css_class'],
                    style=self.config['pygments_style'],
                    noclasses=self.config['noclasses'],
                    tab_length=self.md.tab_length,
                    use_pygments=self.config['use_pygments']
                )
                placeholder = self.md.htmlStash.store(code.hilite())
                # Clear codeblock in etree instance
                block.clear()
                # Change to p element which will later
                # be removed when inserting raw html
                block.tag = 'p'
                block.text = placeholder


class CodeHiliteExtension(Extension):
    """ Add source code hilighting to markdown codeblocks. """

    def __init__(self, **kwargs):
        # define default configs
        self.config = {
            'linenums': [None,
                         "Use lines numbers. True=yes, False=no, None=auto"],
            'guess_lang': [True,
                           "Automatic language detection - Default: True"],
            'css_class': ["codehilite",
                          "Set class name for wrapper <div> - "
                          "Default: codehilite"],
            'pygments_style': ['default',
                               'Pygments HTML Formatter Style '
                               '(Colorscheme) - Default: default'],
            'noclasses': [False,
                          'Use inline styles instead of CSS classes - '
                          'Default false'],
            'use_pygments': [True,
                             'Use Pygments to Highlight code blocks. '
                             'Disable if using a JavaScript library. '
                             'Default: True']
            }

        super(CodeHiliteExtension, self).__init__(**kwargs)

    def extendMarkdown(self, md):
        """ Add HilitePostprocessor to Markdown instance. """
        hiliter = HiliteTreeprocessor(md)
        hiliter.config = self.getConfigs()
        md.treeprocessors.register(hiliter, 'hilite', 30)

        md.registerExtension(self)


def makeExtension(**kwargs):  # pragma: no cover
    return CodeHiliteExtension(**kwargs)
