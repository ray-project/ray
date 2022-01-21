# Originally from:
# github.com/pytorch/tutorials/blob/60d6ef365e36f3ba82c2b61bf32cc40ac4e86c7b/custom_directives.py # noqa
from docutils.parsers.rst import Directive, directives
from docutils.statemachine import StringList
from docutils import nodes
import os
import sphinx_gallery

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

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
        :tooltip: I am writing this tutorial to focus specifically on NLP.
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
        # Cutoff the `tooltip` after 195 chars.
        if "tooltip" in self.options:
            tooltip = self.options["tooltip"]
            if len(self.options["tooltip"]) > 195:
                tooltip = tooltip[:195] + "..."
        else:
            raise ValueError("Need to provide :tooltip: under "
                             "`.. customgalleryitem::`.")

        # Generate `thumbnail` used in the gallery.
        if "figure" in self.options:
            env = self.state.document.settings.env
            rel_figname, figname = env.relfn2path(self.options["figure"])

            thumb_dir = os.path.join(env.srcdir, "_static/thumbs/")
            os.makedirs(thumb_dir, exist_ok=True)
            image_path = os.path.join(thumb_dir, os.path.basename(figname))
            sphinx_gallery.gen_rst.scale_image(figname, image_path, 400, 280)
            thumbnail = os.path.relpath(image_path, env.srcdir)
            # https://stackoverflow.com/questions/52138336/sphinx-reference-to-an-image-from-different-locations
            thumbnail = "/" + thumbnail
        else:
            # "/" is the top level srcdir
            thumbnail = "/_static/img/thumbnails/default.png"

        if "description" in self.options:
            description = self.options["description"]
        else:
            raise ValueError("Need to provide :description: under "
                             "`customgalleryitem::`.")

        thumbnail_rst = GALLERY_TEMPLATE.format(
            tooltip=tooltip, thumbnail=thumbnail, description=description)
        thumbnail = StringList(thumbnail_rst.split("\n"))
        thumb = nodes.paragraph()
        self.state.nested_parse(thumbnail, self.content_offset, thumb)
        return [thumb]


def fix_xgb_lgbm_docs(app, what, name, obj, options, lines):
    """Fix XGBoost-Ray and LightGBM-Ray docstrings.

    For ``app.connect('autodoc-process-docstring')``.
    See https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html

    Removes references to XGBoost ``callback_api`` and sets explicit module
    references to classes and functions that are named the same way in both
    XGBoost-Ray and LightGBM-Ray.
    """

    def _remove_xgboost_refs(replacements: list):
        """Remove ``callback_api`` ref to XGBoost docs.

        Fixes ``undefined label: callback_api (if the link has no caption
        the label must precede a section header)``
        """
        if name.startswith("xgboost_ray"):
            replacements.append((":ref:`callback_api`", "Callback API"))

    def _replace_ray_params(replacements: list):
        """Replaces references to ``RayParams`` with module-specific ones.

        Fixes ``more than one target found for cross-reference 'RayParams'``.
        """
        if name.startswith("xgboost_ray"):
            replacements.append(("RayParams", "xgboost_ray.RayParams"))
        elif name.startswith("lightgbm_ray"):
            replacements.append(("RayParams", "lightgbm_ray.RayParams"))

    replacements = []
    _remove_xgboost_refs(replacements)
    _replace_ray_params(replacements)
    if replacements:
        for i, _ in enumerate(lines):
            for replacement in replacements:
                lines[i] = lines[i].replace(*replacement)
