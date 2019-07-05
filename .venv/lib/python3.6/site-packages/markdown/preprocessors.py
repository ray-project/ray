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

PRE-PROCESSORS
=============================================================================

Preprocessors work on source text before we start doing anything too
complicated.
"""

from __future__ import absolute_import
from __future__ import unicode_literals
from . import util
import re


def build_preprocessors(md, **kwargs):
    """ Build the default set of preprocessors used by Markdown. """
    preprocessors = util.Registry()
    preprocessors.register(NormalizeWhitespace(md), 'normalize_whitespace', 30)
    preprocessors.register(HtmlBlockPreprocessor(md), 'html_block', 20)
    preprocessors.register(ReferencePreprocessor(md), 'reference', 10)
    return preprocessors


class Preprocessor(util.Processor):
    """
    Preprocessors are run after the text is broken into lines.

    Each preprocessor implements a "run" method that takes a pointer to a
    list of lines of the document, modifies it as necessary and returns
    either the same pointer or a pointer to a new list.

    Preprocessors must extend markdown.Preprocessor.

    """
    def run(self, lines):
        """
        Each subclass of Preprocessor should override the `run` method, which
        takes the document as a list of strings split by newlines and returns
        the (possibly modified) list of lines.

        """
        pass  # pragma: no cover


class NormalizeWhitespace(Preprocessor):
    """ Normalize whitespace for consistent parsing. """

    def run(self, lines):
        source = '\n'.join(lines)
        source = source.replace(util.STX, "").replace(util.ETX, "")
        source = source.replace("\r\n", "\n").replace("\r", "\n") + "\n\n"
        source = source.expandtabs(self.md.tab_length)
        source = re.sub(r'(?<=\n) +\n', '\n', source)
        return source.split('\n')


class HtmlBlockPreprocessor(Preprocessor):
    """Remove html blocks from the text and store them for later retrieval."""

    right_tag_patterns = ["</%s>", "%s>"]
    attrs_pattern = r"""
        \s+(?P<attr>[^>"'/= ]+)=(?P<q>['"])(?P<value>.*?)(?P=q) # attr="value"
        |                                                       # OR
        \s+(?P<attr1>[^>"'/= ]+)=(?P<value1>[^> ]+)             # attr=value
        |                                                       # OR
        \s+(?P<attr2>[^>"'/= ]+)                                # attr
        """
    left_tag_pattern = r'^\<(?P<tag>[^> ]+)(?P<attrs>(%s)*)\s*\/?\>?' % \
                       attrs_pattern
    attrs_re = re.compile(attrs_pattern, re.VERBOSE)
    left_tag_re = re.compile(left_tag_pattern, re.VERBOSE)
    markdown_in_raw = False

    def _get_left_tag(self, block):
        m = self.left_tag_re.match(block)
        if m:
            tag = m.group('tag')
            raw_attrs = m.group('attrs')
            attrs = {}
            if raw_attrs:
                for ma in self.attrs_re.finditer(raw_attrs):
                    if ma.group('attr'):
                        if ma.group('value'):
                            attrs[ma.group('attr').strip()] = ma.group('value')
                        else:
                            attrs[ma.group('attr').strip()] = ""
                    elif ma.group('attr1'):
                        if ma.group('value1'):
                            attrs[ma.group('attr1').strip()] = ma.group(
                                'value1'
                            )
                        else:
                            attrs[ma.group('attr1').strip()] = ""
                    elif ma.group('attr2'):
                        attrs[ma.group('attr2').strip()] = ""
            return tag, len(m.group(0)), attrs
        else:
            tag = block[1:].split(">", 1)[0].lower()
            return tag, len(tag)+2, {}

    def _recursive_tagfind(self, ltag, rtag, start_index, block):
        while 1:
            i = block.find(rtag, start_index)
            if i == -1:
                return -1
            j = block.find(ltag, start_index)
            # if no ltag, or rtag found before another ltag, return index
            if (j > i or j == -1):
                return i + len(rtag)
            # another ltag found before rtag, use end of ltag as starting
            # point and search again
            j = block.find('>', j)
            start_index = self._recursive_tagfind(ltag, rtag, j + 1, block)
            if start_index == -1:
                # HTML potentially malformed- ltag has no corresponding
                # rtag
                return -1

    def _get_right_tag(self, left_tag, left_index, block):
        for p in self.right_tag_patterns:
            tag = p % left_tag
            i = self._recursive_tagfind(
                "<%s" % left_tag, tag, left_index, block
            )
            if i > 2:
                return tag.lstrip("<").rstrip(">"), i
        return block.rstrip()[-left_index:-1].lower(), len(block)

    def _equal_tags(self, left_tag, right_tag):
        if left_tag[0] in ['?', '@', '%']:  # handle PHP, etc.
            return True
        if ("/" + left_tag) == right_tag:
            return True
        if (right_tag == "--" and left_tag == "--"):
            return True
        elif left_tag == right_tag[1:] and right_tag[0] == "/":
            return True
        else:
            return False

    def _is_oneliner(self, tag):
        return (tag in ['hr', 'hr/'])

    def _stringindex_to_listindex(self, stringindex, items):
        """
        Same effect as concatenating the strings in items,
        finding the character to which stringindex refers in that string,
        and returning the index of the item in which that character resides.
        """
        items.append('dummy')
        i, count = 0, 0
        while count <= stringindex:
            count += len(items[i])
            i += 1
        return i - 1

    def _nested_markdown_in_html(self, items):
        """Find and process html child elements of the given element block."""
        for i, item in enumerate(items):
            if self.left_tag_re.match(item):
                left_tag, left_index, attrs = \
                    self._get_left_tag(''.join(items[i:]))
                right_tag, data_index = self._get_right_tag(
                    left_tag, left_index, ''.join(items[i:]))
                right_listindex = \
                    self._stringindex_to_listindex(data_index, items[i:]) + i
                if 'markdown' in attrs.keys():
                    items[i] = items[i][left_index:]  # remove opening tag
                    placeholder = self.md.htmlStash.store_tag(
                        left_tag, attrs, i + 1, right_listindex + 1)
                    items.insert(i, placeholder)
                    if len(items) - right_listindex <= 1:  # last nest, no tail
                        right_listindex -= 1
                    items[right_listindex] = items[right_listindex][
                        :-len(right_tag) - 2]  # remove closing tag
                else:  # raw html
                    if len(items) - right_listindex <= 1:  # last element
                        right_listindex -= 1
                    if right_listindex <= i:
                        right_listindex = i + 1
                    placeholder = self.md.htmlStash.store('\n\n'.join(
                        items[i:right_listindex]))
                    del items[i:right_listindex]
                    items.insert(i, placeholder)
        return items

    def run(self, lines):
        text = "\n".join(lines)
        new_blocks = []
        text = text.rsplit("\n\n")
        items = []
        left_tag = ''
        right_tag = ''
        in_tag = False  # flag

        while text:
            block = text[0]
            if block.startswith("\n"):
                block = block[1:]
            text = text[1:]

            if block.startswith("\n"):
                block = block[1:]

            if not in_tag:
                if block.startswith("<") and len(block.strip()) > 1:

                    if block[1:4] == "!--":
                        # is a comment block
                        left_tag, left_index, attrs = "--", 2, {}
                    else:
                        left_tag, left_index, attrs = self._get_left_tag(block)
                    right_tag, data_index = self._get_right_tag(left_tag,
                                                                left_index,
                                                                block)
                    # keep checking conditions below and maybe just append

                    if data_index < len(block) and (self.md.is_block_level(left_tag) or left_tag == '--'):
                        text.insert(0, block[data_index:])
                        block = block[:data_index]

                    if not (self.md.is_block_level(left_tag) or block[1] in ["!", "?", "@", "%"]):
                        new_blocks.append(block)
                        continue

                    if self._is_oneliner(left_tag):
                        new_blocks.append(block.strip())
                        continue

                    if block.rstrip().endswith(">") \
                            and self._equal_tags(left_tag, right_tag):
                        if self.markdown_in_raw and 'markdown' in attrs.keys():
                            block = block[left_index:-len(right_tag) - 2]
                            new_blocks.append(self.md.htmlStash.
                                              store_tag(left_tag, attrs, 0, 2))
                            new_blocks.extend([block])
                        else:
                            new_blocks.append(
                                self.md.htmlStash.store(block.strip()))
                        continue
                    else:
                        # if is block level tag and is not complete
                        if (not self._equal_tags(left_tag, right_tag)) and \
                           (self.md.is_block_level(left_tag) or left_tag == "--"):
                            items.append(block.strip())
                            in_tag = True
                        else:
                            new_blocks.append(
                                self.md.htmlStash.store(block.strip())
                            )
                        continue

                else:
                    new_blocks.append(block)

            else:
                items.append(block)

                # Need to evaluate all items so we can calculate relative to the left index.
                right_tag, data_index = self._get_right_tag(left_tag, left_index, ''.join(items))
                # Adjust data_index: relative to items -> relative to last block
                prev_block_length = 0
                for item in items[:-1]:
                    prev_block_length += len(item)
                data_index -= prev_block_length

                if self._equal_tags(left_tag, right_tag):
                    # if find closing tag

                    if data_index < len(block):
                        # we have more text after right_tag
                        items[-1] = block[:data_index]
                        text.insert(0, block[data_index:])

                    in_tag = False
                    if self.markdown_in_raw and 'markdown' in attrs.keys():
                        items[0] = items[0][left_index:]
                        items[-1] = items[-1][:-len(right_tag) - 2]
                        if items[len(items) - 1]:  # not a newline/empty string
                            right_index = len(items) + 3
                        else:
                            right_index = len(items) + 2
                        new_blocks.append(self.md.htmlStash.store_tag(
                            left_tag, attrs, 0, right_index))
                        placeholderslen = len(self.md.htmlStash.tag_data)
                        new_blocks.extend(
                            self._nested_markdown_in_html(items))
                        nests = len(self.md.htmlStash.tag_data) - \
                            placeholderslen
                        self.md.htmlStash.tag_data[-1 - nests][
                            'right_index'] += nests - 2
                    else:
                        new_blocks.append(
                            self.md.htmlStash.store('\n\n'.join(items)))
                    items = []

        if items:
            if self.markdown_in_raw and 'markdown' in attrs.keys():
                items[0] = items[0][left_index:]
                items[-1] = items[-1][:-len(right_tag) - 2]
                if items[len(items) - 1]:  # not a newline/empty string
                    right_index = len(items) + 3
                else:
                    right_index = len(items) + 2
                new_blocks.append(
                    self.md.htmlStash.store_tag(
                        left_tag, attrs, 0, right_index))
                placeholderslen = len(self.md.htmlStash.tag_data)
                new_blocks.extend(self._nested_markdown_in_html(items))
                nests = len(self.md.htmlStash.tag_data) - placeholderslen
                self.md.htmlStash.tag_data[-1 - nests][
                    'right_index'] += nests - 2
            else:
                new_blocks.append(
                    self.md.htmlStash.store('\n\n'.join(items)))
            new_blocks.append('\n')

        new_text = "\n\n".join(new_blocks)
        return new_text.split("\n")


class ReferencePreprocessor(Preprocessor):
    """ Remove reference definitions from text and store for later use. """

    TITLE = r'[ ]*(\"(.*)\"|\'(.*)\'|\((.*)\))[ ]*'
    RE = re.compile(
        r'^[ ]{0,3}\[([^\]]*)\]:\s*([^ ]*)[ ]*(%s)?$' % TITLE, re.DOTALL
    )
    TITLE_RE = re.compile(r'^%s$' % TITLE)

    def run(self, lines):
        new_text = []
        while lines:
            line = lines.pop(0)
            m = self.RE.match(line)
            if m:
                id = m.group(1).strip().lower()
                link = m.group(2).lstrip('<').rstrip('>')
                t = m.group(5) or m.group(6) or m.group(7)
                if not t:
                    # Check next line for title
                    tm = self.TITLE_RE.match(lines[0])
                    if tm:
                        lines.pop(0)
                        t = tm.group(2) or tm.group(3) or tm.group(4)
                self.md.references[id] = (link, t)
                # Preserve the line to prevent raw HTML indexing issue.
                # https://github.com/Python-Markdown/markdown/issues/584
                new_text.append('')
            else:
                new_text.append(line)

        return new_text  # + "\n"
