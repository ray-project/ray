from docutils import nodes, utils, frontend
from docutils.parsers.rst import directives, Parser
from sphinx.util.docutils import SphinxDirective


class URLQueryParamRefNode(nodes.General, nodes.Element):
    """Node type for references that need URL query parameters."""


class WrapperNode(nodes.paragraph):
    """A wrapper node for URL query param references.

    Sphinx will not build if you don't wrap a reference in a paragraph. And a <div>
    is needed anyway for styling purposes.
    """

    def visit(self, node):
        if "class" not in node:
            raise ValueError(
                "'class' attribute must be set on the WrapperNode for a "
                "URL query parameter reference."
            )
        self.body.append(self.starttag(node, "div", CLASS=node["class"]))

    def depart(self, node):
        self.body.append("</div>")


class URLQueryParamRefDirective(SphinxDirective):
    """Sphinx directive to insert a reference with query parameters."""

    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = True
    has_content = True
    option_spec = {
        "parameters": directives.unchanged_required,
        "classes": directives.class_option,
        "ref-type": (
            lambda choice: directives.choice(choice, ["any", "ref", "doc", "myst"])
        ),
    }

    def run(self):
        ref_type = self.options.get("ref-type", "any")
        content = self.content.data
        reftarget = directives.uri(self.arguments[0])
        return [
            URLQueryParamRefNode(
                {
                    "docname": self.env.docname,
                    "parameters": self.options.get("parameters", None),
                    "classes": self.options.get("classes", []),
                    "reftarget": reftarget,
                    "refdocname": self.env.docname,
                    "refdomain": "std" if ref_type in {"ref", "doc"} else "",
                    "reftype": ref_type,
                    "refexplicit": content if content else reftarget,
                    "refwarn": True,
                }
            )
        ]


def on_doctree_resolved(app, doctree, docname):
    """Replace URLQueryParamRefNode instances with real references.

    Any text that lives inside a URLQueryParamRefNode is parsed as usual.

    Args:
        app: Sphinx application
        doctree: Doctree which has just been resolved
        docname: Name of the document containing the reference nodes
    """
    parser = Parser()
    for node in doctree.traverse(URLQueryParamRefNode):
        tmp_node = utils.new_document(
            "Content nested under URLQueryParamRefNode",
            frontend.OptionParser(components=[Parser]).get_default_values(),
        )
        text = "\n".join(node.rawsource["refexplicit"])

        # Parse all child RST as usual, then append any parsed nodes to the
        # reference node.
        parser.parse(text, tmp_node)
        ref_node = nodes.reference(
            rawsource=text,
            text="",
        )
        for child in tmp_node.children:
            ref_node.append(child)

        # Pass all URLQueryParamRefNode attributes to ref node;
        # possibly not necessary
        for key, value in node.rawsource.items():
            ref_node[key] = value

        # Need to update refuri of ref node to include URL query parameters
        ref_node["refuri"] = (
            app.builder.get_relative_uri(
                docname,
                node.rawsource["reftarget"],
            )
            + node.rawsource["parameters"]
        )

        # Need to wrap the node in a paragraph for Sphinx to build
        wrapper = WrapperNode()
        wrapper["class"] = "query-param-ref-wrapper"
        wrapper += ref_node
        node.replace_self([wrapper])


def setup(app):
    app.add_directive("query-param-ref", URLQueryParamRefDirective)
    app.connect("doctree-resolved", on_doctree_resolved)
    app.add_node(WrapperNode, html=(WrapperNode.visit, WrapperNode.depart))
    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
