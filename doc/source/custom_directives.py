# Originally from:
# github.com/pytorch/tutorials/blob/60d6ef365e36f3ba82c2b61bf32cc40ac4e86c7b/custom_directives.py # noqa
from docutils.parsers.rst import Directive, directives
from docutils.statemachine import StringList
from docutils import nodes
import re
import os
import sphinx_gallery

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class IncludeDirective(Directive):
    """Include source file without docstring at the top of file.

    Implementation just replaces the first docstring found in file
    with '' once.

    Example usage:

    .. includenodoc:: /beginner/examples_tensor/two_layer_net_tensor.py

    """

    # defines the parameter the directive expects
    # directives.unchanged means you get the raw value from RST
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = True
    has_content = False
    add_index = False

    docstring_pattern = r'"""(?P<docstring>(?:.|[\r\n])*?)"""\n'
    docstring_regex = re.compile(docstring_pattern)

    def run(self):
        document = self.state.document
        env = document.settings.env
        rel_filename, filename = env.relfn2path(self.arguments[0])

        try:
            text = open(filename).read()
            text_no_docstring = self.docstring_regex.sub('', text, count=1)

            code_block = nodes.literal_block(text=text_no_docstring)
            return [code_block]
        except FileNotFoundError as e:
            print(e)
            return []


class GalleryItemDirective(Directive):
    """
    Create a sphinx gallery thumbnail for insertion anywhere in docs.

    Optionally, you can specify the custom figure and intro/tooltip for the
    thumbnail.

    Example usage:

    .. galleryitem:: intermediate/char_rnn_generation_tutorial.py
        :figure: _static/img/char_rnn_generation.png
        :intro: Put your custom intro here.

    If figure is specified, a thumbnail will be made out of it and stored in
    _static/thumbs. Therefore, consider _static/thumbs as a 'built' directory.
    """

    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = True
    option_spec = {
        "figure": directives.unchanged,
        "intro": directives.unchanged
    }
    has_content = False
    add_index = False

    def run(self):
        args = self.arguments
        fname = args[-1]

        env = self.state.document.settings.env
        fname, abs_fname = env.relfn2path(fname)
        basename = os.path.basename(fname)
        dirname = os.path.dirname(fname)

        try:
            if "intro" in self.options:
                intro = self.options["intro"][:195] + "..."
            else:
                _, blocks = sphinx_gallery.gen_rst.split_code_and_text_blocks(
                    abs_fname)
                intro, _ = sphinx_gallery.gen_rst.extract_intro_and_title(
                    abs_fname, blocks[0][1])

            thumbnail_rst = sphinx_gallery.backreferences._thumbnail_div(
                dirname, basename, intro)

            if "figure" in self.options:
                rel_figname, figname = env.relfn2path(self.options["figure"])
                save_figname = os.path.join("_static/thumbs/",
                                            os.path.basename(figname))

                try:
                    os.makedirs("_static/thumbs")
                except OSError:
                    pass

                sphinx_gallery.gen_rst.scale_image(figname, save_figname, 400,
                                                   280)
                # replace figure in rst with simple regex
                thumbnail_rst = re.sub(r"..\sfigure::\s.*\.png",
                                       ".. figure:: /{}".format(save_figname),
                                       thumbnail_rst)

            thumbnail = StringList(thumbnail_rst.split("\n"))
            thumb = nodes.paragraph()
            self.state.nested_parse(thumbnail, self.content_offset, thumb)

            return [thumb]
        except FileNotFoundError as e:
            print(e)
            return []


GALLERY_TEMPLATE = """
.. raw:: html

    <div class="sphx-glr-thumbcontainer" tooltip="{tooltip}">

.. only:: html

    .. figure:: {thumbnail}

        {description}

.. raw:: html

    </div>
"""


class CustomGalleryItemDirective(Directive):
    """Create a sphinx gallery style thumbnail.

    tooltip and figure are self explanatory. Description could be a link to
    a document like in below example.

    Example usage:

    .. customgalleryitem::
        :tooltip: I am writing this tutorial to focus specifically on NLP for people who have never written code in any deep learning framework
        :figure: /_static/img/thumbnails/babel.jpg
        :description: :doc:`/beginner/deep_learning_nlp_tutorial`

    If figure is specified, a thumbnail will be made out of it and stored in
    _static/thumbs. Therefore, consider _static/thumbs as a "built" directory.
    """

    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = True
    option_spec = {
        "tooltip": directives.unchanged,
        "figure": directives.unchanged,
        "description": directives.unchanged
    }

    has_content = False
    add_index = False

    def run(self):
        try:
            if "tooltip" in self.options:
                tooltip = self.options["tooltip"][:195] + "..."
            else:
                raise ValueError("tooltip not found")

            if "figure" in self.options:
                env = self.state.document.settings.env
                rel_figname, figname = env.relfn2path(self.options["figure"])
                thumbnail = os.path.join("_static/thumbs/",
                                         os.path.basename(figname))

                try:
                    os.makedirs("_static/thumbs")
                except FileExistsError:
                    pass

                sphinx_gallery.gen_rst.scale_image(figname, thumbnail, 400,
                                                   280)
            else:
                thumbnail = "/_static/img/thumbnails/default.png"

            if "description" in self.options:
                description = self.options["description"]
            else:
                raise ValueError("description not doc found")

        except FileNotFoundError as e:
            print(e)
            return []
        except ValueError as e:
            print(e)
            raise
            return []

        thumbnail_rst = GALLERY_TEMPLATE.format(
            tooltip=tooltip, thumbnail=thumbnail, description=description)
        thumbnail = StringList(thumbnail_rst.split("\n"))
        thumb = nodes.paragraph()
        self.state.nested_parse(thumbnail, self.content_offset, thumb)
        return [thumb]
