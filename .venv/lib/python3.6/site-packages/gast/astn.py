import ast
import gast


def _generate_translators(to):

    class Translator(ast.NodeTransformer):

        def _visit(self, node):
            if isinstance(node, list):
                return [self._visit(n) for n in node]
            elif isinstance(node, ast.AST):
                return self.visit(node)
            else:
                return node

        def generic_visit(self, node):
            cls = type(node).__name__
            # handle nodes that are not part of the AST
            if not hasattr(to, cls):
                return
            new_node = getattr(to, cls)()
            for field in node._fields:
                setattr(new_node, field, self._visit(getattr(node, field)))
            for attr in getattr(node, '_attributes'):
                if hasattr(node, attr):
                    setattr(new_node, attr, getattr(node, attr))
            return new_node

    return Translator

AstToGAst = _generate_translators(gast)
GAstToAst = _generate_translators(ast)
