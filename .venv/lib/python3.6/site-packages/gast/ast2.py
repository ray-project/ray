from astn import AstToGAst, GAstToAst
import ast
import gast


class Ast2ToGAst(AstToGAst):

    # stmt
    def visit_FunctionDef(self, node):
        new_node = gast.FunctionDef(
            self._visit(node.name),
            self._visit(node.args),
            self._visit(node.body),
            self._visit(node.decorator_list),
            None,  # returns
        )
        ast.copy_location(new_node, node)
        return new_node

    def visit_ClassDef(self, node):
        new_node = gast.ClassDef(
            self._visit(node.name),
            self._visit(node.bases),
            [],  # keywords
            self._visit(node.body),
            self._visit(node.decorator_list),
        )

        ast.copy_location(new_node, node)
        return new_node

    def visit_With(self, node):
        new_node = gast.With(
            [gast.withitem(
                self._visit(node.context_expr),
                self._visit(node.optional_vars)
            )],
            self._visit(node.body)
        )
        ast.copy_location(new_node, node)
        return new_node

    def visit_Raise(self, node):
        ntype = self._visit(node.type)
        ninst = self._visit(node.inst)
        ntback = self._visit(node.tback)

        what = ntype

        if ninst is not None:
            what = gast.Call(ntype, [ninst], [])
            ast.copy_location(what, node)

        if ntback is not None:
            attr = gast.Attribute(what, 'with_traceback', gast.Load())
            ast.copy_location(attr, node)

            what = gast.Call(
                attr,
                [ntback],
                []
            )
            ast.copy_location(what, node)

        new_node = gast.Raise(what, None)

        ast.copy_location(new_node, node)
        return new_node

    def visit_TryExcept(self, node):
        new_node = gast.Try(
            self._visit(node.body),
            self._visit(node.handlers),
            self._visit(node.orelse),
            []  # finalbody
        )
        ast.copy_location(new_node, node)
        return new_node

    def visit_TryFinally(self, node):
        new_node = gast.Try(
            self._visit(node.body),
            [],  # handlers
            [],  # orelse
            self._visit(node.finalbody)
        )
        ast.copy_location(new_node, node)
        return new_node

    # expr

    def visit_Name(self, node):
        new_node = gast.Name(
            self._visit(node.id),
            self._visit(node.ctx),
            None,
        )
        ast.copy_location(new_node, node)
        return new_node

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

    def visit_comprehension(self, node):
        new_node = gast.comprehension(
            target=self._visit(node.target),
            iter=self._visit(node.iter),
            ifs=self._visit(node.ifs),
            is_async=0,
        )
        return ast.copy_location(new_node, node)

    # arguments
    def visit_arguments(self, node):
        if node.vararg:
            vararg = ast.Name(node.vararg, ast.Param())
            ast.copy_location(vararg, node)
        else:
            vararg = None

        if node.kwarg:
            kwarg = ast.Name(node.kwarg, ast.Param())
            ast.copy_location(kwarg, node)
        else:
            kwarg = None
        new_node = gast.arguments(
            self._visit(node.args),
            self._visit(vararg),
            [],  # kwonlyargs
            [],  # kw_defaults
            self._visit(kwarg),
            self._visit(node.defaults),
        )
        return new_node


class GAstToAst2(GAstToAst):

    # stmt
    def visit_FunctionDef(self, node):
        new_node = ast.FunctionDef(
            self._visit(node.name),
            self._visit(node.args),
            self._visit(node.body),
            self._visit(node.decorator_list),
        )
        ast.copy_location(new_node, node)
        return new_node

    def visit_ClassDef(self, node):
        new_node = ast.ClassDef(
            self._visit(node.name),
            self._visit(node.bases),
            self._visit(node.body),
            self._visit(node.decorator_list),
        )

        ast.copy_location(new_node, node)
        return new_node

    def visit_With(self, node):
        new_node = ast.With(
            self._visit(node.items[0].context_expr),
            self._visit(node.items[0].optional_vars),
            self._visit(node.body)
        )
        ast.copy_location(new_node, node)
        return new_node

    def visit_Raise(self, node):
        if isinstance(node.exc, gast.Call) and \
           isinstance(node.exc.func, gast.Attribute) and \
           node.exc.func.attr == 'with_traceback':
            raised = self._visit(node.exc.func.value)
            traceback = self._visit(node.exc.args[0])
        else:
            raised = self._visit(node.exc)
            traceback = None
        new_node = ast.Raise(raised, None, traceback)
        ast.copy_location(new_node, node)
        return new_node

    def visit_Try(self, node):
        if node.finalbody:
            new_node = ast.TryFinally(
                self._visit(node.body),
                self._visit(node.finalbody)
            )
        else:
            new_node = ast.TryExcept(
                self._visit(node.body),
                self._visit(node.handlers),
                self._visit(node.orelse),
            )
        ast.copy_location(new_node, node)
        return new_node

    # expr

    def visit_Name(self, node):
        new_node = ast.Name(
            self._visit(node.id),
            self._visit(node.ctx),
        )
        ast.copy_location(new_node, node)
        return new_node

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

    def visit_arg(self, node):
        new_node = ast.Name(node.arg, ast.Param())
        ast.copy_location(new_node, node)
        return new_node

    # arguments
    def visit_arguments(self, node):
        vararg = node.vararg and node.vararg.id
        kwarg = node.kwarg and node.kwarg.id

        new_node = ast.arguments(
            self._visit(node.args),
            self._visit(vararg),
            self._visit(kwarg),
            self._visit(node.defaults),
        )
        return new_node


def ast_to_gast(node):
    return Ast2ToGAst().visit(node)


def gast_to_ast(node):
    return GAstToAst2().visit(node)
