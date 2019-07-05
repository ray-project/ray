"""
Footnotes Extension for Python-Markdown
=======================================

Adds footnote handling to Python-Markdown.

See <https://Python-Markdown.github.io/extensions/footnotes>
for documentation.

Copyright The Python Markdown Project

License: [BSD](http://www.opensource.org/licenses/bsd-license.php)

"""

from __future__ import absolute_import
from __future__ import unicode_literals
from . import Extension
from ..preprocessors import Preprocessor
from ..inlinepatterns import InlineProcessor
from ..treeprocessors import Treeprocessor
from ..postprocessors import Postprocessor
from .. import util
from collections import OrderedDict
import re
import copy

FN_BACKLINK_TEXT = util.STX + "zz1337820767766393qq" + util.ETX
NBSP_PLACEHOLDER = util.STX + "qq3936677670287331zz" + util.ETX
DEF_RE = re.compile(r'[ ]{0,3}\[\^([^\]]*)\]:\s*(.*)')
TABBED_RE = re.compile(r'((\t)|(    ))(.*)')
RE_REF_ID = re.compile(r'(fnref)(\d+)')


class FootnoteExtension(Extension):
    """ Footnote Extension. """

    def __init__(self, **kwargs):
        """ Setup configs. """

        self.config = {
            'PLACE_MARKER':
                ["///Footnotes Go Here///",
                 "The text string that marks where the footnotes go"],
            'UNIQUE_IDS':
                [False,
                 "Avoid name collisions across "
                 "multiple calls to reset()."],
            "BACKLINK_TEXT":
                ["&#8617;",
                 "The text string that links from the footnote "
                 "to the reader's place."],
            "BACKLINK_TITLE":
                ["Jump back to footnote %d in the text",
                 "The text string used for the title HTML attribute "
                 "of the backlink. %d will be replaced by the "
                 "footnote number."],
            "SEPARATOR":
                [":",
                 "Footnote separator."]
        }
        super(FootnoteExtension, self).__init__(**kwargs)

        # In multiple invocations, emit links that don't get tangled.
        self.unique_prefix = 0
        self.found_refs = {}
        self.used_refs = set()

        self.reset()

    def extendMarkdown(self, md):
        """ Add pieces to Markdown. """
        md.registerExtension(self)
        self.parser = md.parser
        self.md = md
        # Insert a preprocessor before ReferencePreprocessor
        md.preprocessors.register(FootnotePreprocessor(self), 'footnote', 15)

        # Insert an inline pattern before ImageReferencePattern
        FOOTNOTE_RE = r'\[\^([^\]]*)\]'  # blah blah [^1] blah
        md.inlinePatterns.register(FootnoteInlineProcessor(FOOTNOTE_RE, self), 'footnote', 175)
        # Insert a tree-processor that would actually add the footnote div
        # This must be before all other treeprocessors (i.e., inline and
        # codehilite) so they can run on the the contents of the div.
        md.treeprocessors.register(FootnoteTreeprocessor(self), 'footnote', 50)

        # Insert a tree-processor that will run after inline is done.
        # In this tree-processor we want to check our duplicate footnote tracker
        # And add additional backrefs to the footnote pointing back to the
        # duplicated references.
        md.treeprocessors.register(FootnotePostTreeprocessor(self), 'footnote-duplicate', 15)

        # Insert a postprocessor after amp_substitute processor
        md.postprocessors.register(FootnotePostprocessor(self), 'footnote', 25)

    def reset(self):
        """ Clear footnotes on reset, and prepare for distinct document. """
        self.footnotes = OrderedDict()
        self.unique_prefix += 1
        self.found_refs = {}
        self.used_refs = set()

    def unique_ref(self, reference, found=False):
        """ Get a unique reference if there are duplicates. """
        if not found:
            return reference

        original_ref = reference
        while reference in self.used_refs:
            ref, rest = reference.split(self.get_separator(), 1)
            m = RE_REF_ID.match(ref)
            if m:
                reference = '%s%d%s%s' % (m.group(1), int(m.group(2))+1, self.get_separator(), rest)
            else:
                reference = '%s%d%s%s' % (ref, 2, self.get_separator(), rest)

        self.used_refs.add(reference)
        if original_ref in self.found_refs:
            self.found_refs[original_ref] += 1
        else:
            self.found_refs[original_ref] = 1
        return reference

    def findFootnotesPlaceholder(self, root):
        """ Return ElementTree Element that contains Footnote placeholder. """
        def finder(element):
            for child in element:
                if child.text:
                    if child.text.find(self.getConfig("PLACE_MARKER")) > -1:
                        return child, element, True
                if child.tail:
                    if child.tail.find(self.getConfig("PLACE_MARKER")) > -1:
                        return child, element, False
                child_res = finder(child)
                if child_res is not None:
                    return child_res
            return None

        res = finder(root)
        return res

    def setFootnote(self, id, text):
        """ Store a footnote for later retrieval. """
        self.footnotes[id] = text

    def get_separator(self):
        """ Get the footnote separator. """
        return self.getConfig("SEPARATOR")

    def makeFootnoteId(self, id):
        """ Return footnote link id. """
        if self.getConfig("UNIQUE_IDS"):
            return 'fn%s%d-%s' % (self.get_separator(), self.unique_prefix, id)
        else:
            return 'fn%s%s' % (self.get_separator(), id)

    def makeFootnoteRefId(self, id, found=False):
        """ Return footnote back-link id. """
        if self.getConfig("UNIQUE_IDS"):
            return self.unique_ref('fnref%s%d-%s' % (self.get_separator(), self.unique_prefix, id), found)
        else:
            return self.unique_ref('fnref%s%s' % (self.get_separator(), id), found)

    def makeFootnotesDiv(self, root):
        """ Return div of footnotes as et Element. """

        if not list(self.footnotes.keys()):
            return None

        div = util.etree.Element("div")
        div.set('class', 'footnote')
        util.etree.SubElement(div, "hr")
        ol = util.etree.SubElement(div, "ol")
        surrogate_parent = util.etree.Element("div")

        for index, id in enumerate(self.footnotes.keys(), start=1):
            li = util.etree.SubElement(ol, "li")
            li.set("id", self.makeFootnoteId(id))
            # Parse footnote with surrogate parent as li cannot be used.
            # List block handlers have special logic to deal with li.
            # When we are done parsing, we will copy everything over to li.
            self.parser.parseChunk(surrogate_parent, self.footnotes[id])
            for el in list(surrogate_parent):
                li.append(el)
                surrogate_parent.remove(el)
            backlink = util.etree.Element("a")
            backlink.set("href", "#" + self.makeFootnoteRefId(id))
            backlink.set("class", "footnote-backref")
            backlink.set(
                "title",
                self.getConfig("BACKLINK_TITLE") % (index)
            )
            backlink.text = FN_BACKLINK_TEXT

            if len(li):
                node = li[-1]
                if node.tag == "p":
                    node.text = node.text + NBSP_PLACEHOLDER
                    node.append(backlink)
                else:
                    p = util.etree.SubElement(li, "p")
                    p.append(backlink)
        return div


class FootnotePreprocessor(Preprocessor):
    """ Find all footnote references and store for later use. """

    def __init__(self, footnotes):
        self.footnotes = footnotes

    def run(self, lines):
        """
        Loop through lines and find, set, and remove footnote definitions.

        Keywords:

        * lines: A list of lines of text

        Return: A list of lines of text with footnote definitions removed.

        """
        newlines = []
        i = 0
        while True:
            m = DEF_RE.match(lines[i])
            if m:
                fn, _i = self.detectTabbed(lines[i+1:])
                fn.insert(0, m.group(2))
                i += _i-1  # skip past footnote
                footnote = "\n".join(fn)
                self.footnotes.setFootnote(m.group(1), footnote.rstrip())
                # Preserve a line for each block to prevent raw HTML indexing issue.
                # https://github.com/Python-Markdown/markdown/issues/584
                num_blocks = (len(footnote.split('\n\n')) * 2)
                newlines.extend([''] * (num_blocks))
            else:
                newlines.append(lines[i])
            if len(lines) > i+1:
                i += 1
            else:
                break
        return newlines

    def detectTabbed(self, lines):
        """ Find indented text and remove indent before further proccesing.

        Keyword arguments:

        * lines: an array of strings

        Returns: a list of post processed items and the index of last line.

        """
        items = []
        blank_line = False  # have we encountered a blank line yet?
        i = 0  # to keep track of where we are

        def detab(line):
            match = TABBED_RE.match(line)
            if match:
                return match.group(4)

        for line in lines:
            if line.strip():  # Non-blank line
                detabbed_line = detab(line)
                if detabbed_line:
                    items.append(detabbed_line)
                    i += 1
                    continue
                elif not blank_line and not DEF_RE.match(line):
                    # not tabbed but still part of first par.
                    items.append(line)
                    i += 1
                    continue
                else:
                    return items, i+1

            else:  # Blank line: _maybe_ we are done.
                blank_line = True
                i += 1  # advance

                # Find the next non-blank line
                for j in range(i, len(lines)):
                    if lines[j].strip():
                        next_line = lines[j]
                        break
                    else:
                        # Include extreaneous padding to prevent raw HTML
                        # parsing issue: https://github.com/Python-Markdown/markdown/issues/584
                        items.append("")
                        i += 1
                else:
                    break  # There is no more text; we are done.

                # Check if the next non-blank line is tabbed
                if detab(next_line):  # Yes, more work to do.
                    items.append("")
                    continue
                else:
                    break  # No, we are done.
        else:
            i += 1

        return items, i


class FootnoteInlineProcessor(InlineProcessor):
    """ InlinePattern for footnote markers in a document's body text. """

    def __init__(self, pattern, footnotes):
        super(FootnoteInlineProcessor, self).__init__(pattern)
        self.footnotes = footnotes

    def handleMatch(self, m, data):
        id = m.group(1)
        if id in self.footnotes.footnotes.keys():
            sup = util.etree.Element("sup")
            a = util.etree.SubElement(sup, "a")
            sup.set('id', self.footnotes.makeFootnoteRefId(id, found=True))
            a.set('href', '#' + self.footnotes.makeFootnoteId(id))
            a.set('class', 'footnote-ref')
            a.text = util.text_type(list(self.footnotes.footnotes.keys()).index(id) + 1)
            return sup, m.start(0), m.end(0)
        else:
            return None, None, None


class FootnotePostTreeprocessor(Treeprocessor):
    """ Amend footnote div with duplicates. """

    def __init__(self, footnotes):
        self.footnotes = footnotes

    def add_duplicates(self, li, duplicates):
        """ Adjust current li and add the duplicates: fnref2, fnref3, etc. """
        for link in li.iter('a'):
            # Find the link that needs to be duplicated.
            if link.attrib.get('class', '') == 'footnote-backref':
                ref, rest = link.attrib['href'].split(self.footnotes.get_separator(), 1)
                # Duplicate link the number of times we need to
                # and point the to the appropriate references.
                links = []
                for index in range(2, duplicates + 1):
                    sib_link = copy.deepcopy(link)
                    sib_link.attrib['href'] = '%s%d%s%s' % (ref, index, self.footnotes.get_separator(), rest)
                    links.append(sib_link)
                    self.offset += 1
                # Add all the new duplicate links.
                el = list(li)[-1]
                for l in links:
                    el.append(l)
                break

    def get_num_duplicates(self, li):
        """ Get the number of duplicate refs of the footnote. """
        fn, rest = li.attrib.get('id', '').split(self.footnotes.get_separator(), 1)
        link_id = '%sref%s%s' % (fn, self.footnotes.get_separator(), rest)
        return self.footnotes.found_refs.get(link_id, 0)

    def handle_duplicates(self, parent):
        """ Find duplicate footnotes and format and add the duplicates. """
        for li in list(parent):
            # Check number of duplicates footnotes and insert
            # additional links if needed.
            count = self.get_num_duplicates(li)
            if count > 1:
                self.add_duplicates(li, count)

    def run(self, root):
        """ Crawl the footnote div and add missing duplicate footnotes. """
        self.offset = 0
        for div in root.iter('div'):
            if div.attrib.get('class', '') == 'footnote':
                # Footnotes shoul be under the first orderd list under
                # the footnote div.  So once we find it, quit.
                for ol in div.iter('ol'):
                    self.handle_duplicates(ol)
                    break


class FootnoteTreeprocessor(Treeprocessor):
    """ Build and append footnote div to end of document. """

    def __init__(self, footnotes):
        self.footnotes = footnotes

    def run(self, root):
        footnotesDiv = self.footnotes.makeFootnotesDiv(root)
        if footnotesDiv is not None:
            result = self.footnotes.findFootnotesPlaceholder(root)
            if result:
                child, parent, isText = result
                ind = list(parent).index(child)
                if isText:
                    parent.remove(child)
                    parent.insert(ind, footnotesDiv)
                else:
                    parent.insert(ind + 1, footnotesDiv)
                    child.tail = None
            else:
                root.append(footnotesDiv)


class FootnotePostprocessor(Postprocessor):
    """ Replace placeholders with html entities. """
    def __init__(self, footnotes):
        self.footnotes = footnotes

    def run(self, text):
        text = text.replace(
            FN_BACKLINK_TEXT, self.footnotes.getConfig("BACKLINK_TEXT")
        )
        return text.replace(NBSP_PLACEHOLDER, "&#160;")


def makeExtension(**kwargs):  # pragma: no cover
    """ Return an instance of the FootnoteExtension """
    return FootnoteExtension(**kwargs)
