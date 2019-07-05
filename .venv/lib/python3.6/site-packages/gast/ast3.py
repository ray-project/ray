from gast.astn import AstToGAst, GAstToAst
import gast
import ast
import sys


class Ast3ToGAst(AstToGAst):

    def visit_Name(self, node):
        new_node = gast.Name(
            self._visit(node.id),
            self._visit(node.ctx),
            None,
        )
        return ast.copy_location(new_node, node)

    def visit_arg(self, node):
        new_node = gast.Name(
            self._visit(node.arg),
            gast.Param(),
            self._visit(node.annotation),
        )
        return ast.copy_location(new_node, node)

    def visit_ExceptHandler(self, node):
        if node.name:
            new_node = gast.ExceptHandler(
                self._visit(node.type),
                gast.Name(node.name, gast.Store(), None),
                self._visit(node.body))
            return ast.copy_location(new_node, node)
        else:
            return self.generic_visit(node)

    if sys.version_info.minor < 5:

        def visit_Call(self, node):
            if node.starargs:
                star = gast.Starred(self._visit(node.starargs), gast.Load())
                ast.copy_location(star, node)
                starred = [star]
            else:
                starred = []

            if node.kwargs:
                kwargs = [gast.keyword(None, self._visit(node.kwargs))]
            else:
                kwargs = []

            new_node = gast.Call(
                self._visit(node.func),
                self._visit(node.args) + starred,
                self._visit(node.keywords) + kwargs,
            )
            ast.copy_location(new_node, node)
            return new_node

    if sys.version_info.minor < 6:

        def visit_comprehension(self, node):
            new_node = gast.comprehension(
                target=self._visit(node.target),
                iter=self._visit(node.iter),
                ifs=self._visit(node.ifs),
                is_async=0,
            )
            return ast.copy_location(new_node, node)


class GAstToAst3(GAstToAst):

    def _make_arg(self, node):
        if node is None:
            return None
        new_node = ast.arg(
            self._visit(node.id),
            self._visit(node.annotation),
        )
        return ast.copy_location(new_node, node)

    def visit_Name(self, node):
        new_node = ast.Name(
            self._visit(node.id),
            self._visit(node.ctx),
        )
        return ast.copy_location(new_node, node)

    def visit_ExceptHandler(self, node):
        if node.name:
            new_node = ast.ExceptHandler(
                self._visit(node.type),
                node.name.id,
                self._visit(node.body))
            return ast.copy_location(new_node, node)
        else:
            return self.generic_visit(node)

    if sys.version_info.minor < 5:

        def visit_Call(self, node):
            if node.args and isinstance(node.args[-1], gast.Starred):
                args = node.args[:-1]
                starargs = node.args[-1].value
            else:
                args = node.args
                starargs = None

            if node.keywords and node.keywords[-1].arg is None:
                keywords = node.keywords[:-1]
                kwargs = node.keywords[-1].value
            else:
                keywords = node.keywords
                kwargs = None

            new_node = ast.Call(
                self._visit(node.func),
                self._visit(args),
                self._visit(keywords),
                self._visit(starargs),
                self._visit(kwargs),
            )
            ast.copy_location(new_node, node)
            return new_node

        def visit_ClassDef(self, node):
            self.generic_visit(node)
            new_node = ast.ClassDef(
                name=self._visit(node.name),
                bases=self._visit(node.bases),
                keywords=self._visit(node.keywords),
                body=self._visit(node.body),
                decorator_list=self._visit(node.decorator_list),
                starargs=None,
                kwargs=None,
            )
            return ast.copy_location(new_node, node)

    def visit_arguments(self, node):
        new_node = ast.arguments(
            [self._make_arg(n) for n in node.args],
            self._make_arg(node.vararg),
            [self._make_arg(n) for n in node.kwonlyargs],
            self._visit(node.kw_defaults),
            self._make_arg(node.kwarg),
            self._visit(node.defaults),
        )
        return new_node


def ast_to_gast(node):
    return Ast3ToGAst().visit(node)


def gast_to_ast(node):
    return GAstToAst3().visit(node)
