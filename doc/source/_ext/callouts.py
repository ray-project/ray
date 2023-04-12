from docutils import nodes

from sphinx.util.docutils import SphinxDirective
from sphinx.transforms import SphinxTransform
from docutils.nodes import Node

# BASE_NUM = 2775  # black circles, white numbers
BASE_NUM = 2459  # white circle, black numbers


class CalloutIncludePostTransform(SphinxTransform):
    """Code block post-processor for `literalinclude` blocks used in callouts."""

    default_priority = 400

    def apply(self, **kwargs) -> None:
        visitor = LiteralIncludeVisitor(self.document)
        self.document.walkabout(visitor)


class LiteralIncludeVisitor(nodes.NodeVisitor):
    """Change a literal block upon visiting it."""

    def __init__(self, document: nodes.document) -> None:
        super().__init__(document)

    def unknown_visit(self, node: Node) -> None:
        pass

    def unknown_departure(self, node: Node) -> None:
        pass

    def visit_document(self, node: Node) -> None:
        pass

    def depart_document(self, node: Node) -> None:
        pass

    def visit_start_of_file(self, node: Node) -> None:
        pass

    def depart_start_of_file(self, node: Node) -> None:
        pass

    def visit_literal_block(self, node: nodes.literal_block) -> None:
        if "<1>" in node.rawsource:
            source = str(node.rawsource)
            for i in range(1, 20):
                source = source.replace(
                    f"<{i}>", chr(int(f"0x{BASE_NUM + i}", base=16))
                )
            node.rawsource = source
            node[:] = [nodes.Text(source)]


class callout(nodes.General, nodes.Element):
    """Sphinx callout node."""

    pass


def visit_callout_node(self, node):
    """We pass on node visit to prevent the
    callout being treated as admonition."""
    pass


def depart_callout_node(self, node):
    """Departing a callout node is a no-op, too."""
    pass


class annotations(nodes.Element):
    """Sphinx annotations node."""

    pass


def _replace_numbers(content: str):
    """
    Replaces strings of the form <x> with circled unicode numbers (e.g. ①) as text.

    Args:
        content: Python str from a callout or annotations directive.

    Returns: The formatted content string.
    """
    for i in range(1, 20):
        content.replace(f"<{i}>", chr(int(f"0x{BASE_NUM + i}", base=16)))
    return content


def _parse_recursively(self, node):
    """Utility to recursively parse a node from the Sphinx AST."""
    self.state.nested_parse(self.content, self.content_offset, node)


class CalloutDirective(SphinxDirective):
    """Code callout directive with annotations for Sphinx.

    Use this `callout` directive by wrapping either `code-block` or `literalinclude`
    directives. Each line that's supposed to be equipped with an annotation should
    have an inline comment of the form "# <x>" where x is an integer.

    Afterwards use the `annotations` directive to add annotations to the previously
    defined code labels ("<x>") by using the syntax "<x> my annotation" to produce an
    annotation "my annotation" for x.
    Note that annotation lines have to be separated by a new line, i.e.

    .. annotations::

        <1> First comment followed by a newline,

        <2> second comment after the newline.


    Usage example:
    -------------

    .. callout::

        .. code-block:: python

            from ray import tune
            from ray.tune.search.hyperopt import HyperOptSearch
            import keras

            def objective(config):  # <1>
                ...

            search_space = {"activation": tune.choice(["relu", "tanh"])}  # <2>
            algo = HyperOptSearch()

            tuner = tune.Tuner(  # <3>
                ...
            )
            results = tuner.fit()

        .. annotations::

            <1> Wrap a Keras model in an objective function.

            <2> Define a search space and initialize the search algorithm.

            <3> Start a Tune run that maximizes accuracy.
    """

    has_content = True

    def run(self):
        self.assert_has_content()

        content = self.content
        content = _replace_numbers(content)

        callout_node = callout("\n".join(content))
        _parse_recursively(self, callout_node)

        return [callout_node]


class AnnotationsDirective(SphinxDirective):
    """Annotations directive, which is only used nested within a Callout directive."""

    has_content = True

    def run(self):
        content = self.content
        content = _replace_numbers(content)

        joined_content = "\n".join(content)
        annotations_node = callout(joined_content)
        _parse_recursively(self, annotations_node)

        return [annotations_node]


def setup(app):
    # Add new node types
    app.add_node(
        callout,
        html=(visit_callout_node, depart_callout_node),
        latex=(visit_callout_node, depart_callout_node),
        text=(visit_callout_node, depart_callout_node),
    )
    app.add_node(annotations)

    # Add new directives
    app.add_directive("callout", CalloutDirective)
    app.add_directive("annotations", AnnotationsDirective)

    # Add post-processor
    app.add_post_transform(CalloutIncludePostTransform)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
