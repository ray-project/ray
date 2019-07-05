import sys as _sys
import ast as _ast
from ast import boolop, cmpop, excepthandler, expr, expr_context, operator
from ast import slice, stmt, unaryop, mod, AST


def _make_node(Name, Fields, Attributes, Bases):
    def create_node(self, *args, **kwargs):
        nbparam = len(args) + len(kwargs)
        assert nbparam in (0, len(Fields)), \
            "Bad argument number for {}: {}, expecting {}".\
            format(Name, nbparam, len(Fields))
        self._fields = Fields
        self._attributes = Attributes
        for argname, argval in zip(self._fields, args):
            setattr(self, argname, argval)
        for argname, argval in kwargs.items():
            assert argname in Fields, \
                    "Invalid Keyword argument for {}: {}".format(Name, argname)
            setattr(self, argname, argval)

    setattr(_sys.modules[__name__],
            Name,
            type(Name,
                 Bases,
                 {'__init__': create_node}))

_nodes = {
    # mod
    'Module': (('body',), (), (mod,)),
    'Interactive': (('body',), (), (mod,)),
    'Expression': (('body',), (), (mod,)),
    'Suite': (('body',), (), (mod,)),

    # stmt
    'FunctionDef': (('name', 'args', 'body', 'decorator_list', 'returns',),
                    ('lineno', 'col_offset',),
                    (stmt,)),
    'AsyncFunctionDef': (('name', 'args', 'body',
                          'decorator_list', 'returns',),
                         ('lineno', 'col_offset',),
                         (stmt,)),
    'ClassDef': (('name', 'bases', 'keywords', 'body', 'decorator_list',),
                 ('lineno', 'col_offset',),
                 (stmt,)),
    'Return': (('value',), ('lineno', 'col_offset',),
               (stmt,)),
    'Delete': (('targets',), ('lineno', 'col_offset',),
               (stmt,)),
    'Assign': (('targets', 'value',), ('lineno', 'col_offset',),
               (stmt,)),
    'AugAssign': (('target', 'op', 'value',), ('lineno', 'col_offset',),
                  (stmt,)),
    'AnnAssign': (('target', 'annotation', 'value', 'simple',),
                  ('lineno', 'col_offset',),
                  (stmt,)),
    'Print': (('dest', 'values', 'nl',), ('lineno', 'col_offset',),
              (stmt,)),
    'For': (('target', 'iter', 'body', 'orelse',), ('lineno', 'col_offset',),
            (stmt,)),
    'AsyncFor': (('target', 'iter', 'body', 'orelse',),
                 ('lineno', 'col_offset',),
                 (stmt,)),
    'While': (('test', 'body', 'orelse',), ('lineno', 'col_offset',),
              (stmt,)),
    'If': (('test', 'body', 'orelse',), ('lineno', 'col_offset',),
           (stmt,)),
    'With': (('items', 'body',), ('lineno', 'col_offset',),
             (stmt,)),
    'AsyncWith': (('items', 'body',), ('lineno', 'col_offset',),
                  (stmt,)),
    'Raise': (('exc', 'cause',), ('lineno', 'col_offset',),
              (stmt,)),
    'Try': (('body', 'handlers', 'orelse', 'finalbody',),
            ('lineno', 'col_offset',),
            (stmt,)),
    'Assert': (('test', 'msg',), ('lineno', 'col_offset',),
               (stmt,)),
    'Import': (('names',), ('lineno', 'col_offset',),
               (stmt,)),
    'ImportFrom': (('module', 'names', 'level',), ('lineno', 'col_offset',),
                   (stmt,)),
    'Exec': (('body', 'globals', 'locals',), ('lineno', 'col_offset',),
             (stmt,)),
    'Global': (('names',), ('lineno', 'col_offset',),
               (stmt,)),
    'Nonlocal': (('names',), ('lineno', 'col_offset',),
                 (stmt,)),
    'Expr': (('value',), ('lineno', 'col_offset',),
             (stmt,)),
    'Pass': ((), ('lineno', 'col_offset',),
             (stmt,)),
    'Break': ((), ('lineno', 'col_offset',),
              (stmt,)),
    'Continue': ((), ('lineno', 'col_offset',),
                 (stmt,)),

    # expr

    'BoolOp': (('op', 'values',), ('lineno', 'col_offset',),
               (expr,)),
    'BinOp': (('left', 'op', 'right',), ('lineno', 'col_offset',),
              (expr,)),
    'UnaryOp': (('op', 'operand',), ('lineno', 'col_offset',),
                (expr,)),
    'Lambda': (('args', 'body',), ('lineno', 'col_offset',),
               (expr,)),
    'IfExp': (('test', 'body', 'orelse',), ('lineno', 'col_offset',),
              (expr,)),
    'Dict': (('keys', 'values',), ('lineno', 'col_offset',),
             (expr,)),
    'Set': (('elts',), ('lineno', 'col_offset',),
            (expr,)),
    'ListComp': (('elt', 'generators',), ('lineno', 'col_offset',),
                 (expr,)),
    'SetComp': (('elt', 'generators',), ('lineno', 'col_offset',),
                (expr,)),
    'DictComp': (('key', 'value', 'generators',), ('lineno', 'col_offset',),
                 (expr,)),
    'GeneratorExp': (('elt', 'generators',), ('lineno', 'col_offset',),
                     (expr,)),
    'Await': (('value',), ('lineno', 'col_offset',),
              (expr,)),
    'Yield': (('value',), ('lineno', 'col_offset',),
              (expr,)),
    'YieldFrom': (('value',), ('lineno', 'col_offset',),
                  (expr,)),
    'Compare': (('left', 'ops', 'comparators',), ('lineno', 'col_offset',),
                (expr,)),
    'Call': (('func', 'args', 'keywords',), ('lineno', 'col_offset',),
             (expr,)),
    'Repr': (('value',), ('lineno', 'col_offset',),
             (expr,)),
    'Num': (('n',), ('lineno', 'col_offset',),
            (expr,)),
    'Str': (('s',), ('lineno', 'col_offset',),
            (expr,)),
    'FormattedValue': (('value', 'conversion', 'format_spec',),
                       ('lineno', 'col_offset',), (expr,)),
    'JoinedStr': (('values',), ('lineno', 'col_offset',), (expr,)),
    'Bytes': (('s',), ('lineno', 'col_offset',),
              (expr,)),
    'NameConstant': (('value',), ('lineno', 'col_offset',),
                     (expr,)),
    'Ellipsis': ((), ('lineno', 'col_offset',),
                 (expr,)),
    'Attribute': (('value', 'attr', 'ctx',), ('lineno', 'col_offset',),
                  (expr,)),
    'Subscript': (('value', 'slice', 'ctx',), ('lineno', 'col_offset',),
                  (expr,)),
    'Starred': (('value', 'ctx',), ('lineno', 'col_offset',),
                (expr,)),
    'Name': (('id', 'ctx', 'annotation'), ('lineno', 'col_offset',),
             (expr,)),
    'List': (('elts', 'ctx',), ('lineno', 'col_offset',),
             (expr,)),
    'Tuple': (('elts', 'ctx',), ('lineno', 'col_offset',),
              (expr,)),

    # expr_context
    'Load': ((), (), (expr_context,)),
    'Store': ((), (), (expr_context,)),
    'Del': ((), (), (expr_context,)),
    'AugLoad': ((), (), (expr_context,)),
    'AugStore': ((), (), (expr_context,)),
    'Param': ((), (), (expr_context,)),

    # slice
    'Slice': (('lower', 'upper', 'step'), (), (slice,)),
    'ExtSlice': (('dims',), (), (slice,)),
    'Index': (('value',), (), (slice,)),

    # boolop
    'And': ((), (), (boolop,)),
    'Or': ((), (), (boolop,)),

    # operator
    'Add': ((), (), (operator,)),
    'Sub': ((), (), (operator,)),
    'Mult': ((), (), (operator,)),
    'MatMult': ((), (), (operator,)),
    'Div': ((), (), (operator,)),
    'Mod': ((), (), (operator,)),
    'Pow': ((), (), (operator,)),
    'LShift': ((), (), (operator,)),
    'RShift': ((), (), (operator,)),
    'BitOr': ((), (), (operator,)),
    'BitXor': ((), (), (operator,)),
    'BitAnd': ((), (), (operator,)),
    'FloorDiv': ((), (), (operator,)),

    # unaryop
    'Invert': ((), (), (unaryop, AST,)),
    'Not': ((), (), (unaryop, AST,)),
    'UAdd': ((), (), (unaryop, AST,)),
    'USub': ((), (), (unaryop, AST,)),

    # cmpop
    'Eq': ((), (), (cmpop,)),
    'NotEq': ((), (), (cmpop,)),
    'Lt': ((), (), (cmpop,)),
    'LtE': ((), (), (cmpop,)),
    'Gt': ((), (), (cmpop,)),
    'GtE': ((), (), (cmpop,)),
    'Is': ((), (), (cmpop,)),
    'IsNot': ((), (), (cmpop,)),
    'In': ((), (), (cmpop,)),
    'NotIn': ((), (), (cmpop,)),

    # comprehension
    'comprehension': (('target', 'iter', 'ifs', 'is_async'), (), (AST,)),

    # excepthandler
    'ExceptHandler': (('type', 'name', 'body'), ('lineno', 'col_offset'),
                      (excepthandler,)),

    # arguments
    'arguments': (('args', 'vararg', 'kwonlyargs', 'kw_defaults',
                   'kwarg', 'defaults'), (), (AST,)),
    # keyword
    'keyword': (('arg', 'value'), (), (AST,)),

    # alias
    'alias': (('name', 'asname'), (), (AST,)),

    # withitem
    'withitem': (('context_expr', 'optional_vars'), (), (AST,)),
}

for name, descr in _nodes.items():
    _make_node(name, *descr)

if _sys.version_info.major == 2:
    from .ast2 import ast_to_gast, gast_to_ast
if _sys.version_info.major == 3:
    from .ast3 import ast_to_gast, gast_to_ast


def parse(*args, **kwargs):
    return ast_to_gast(_ast.parse(*args, **kwargs))


def literal_eval(node_or_string):
    if isinstance(node_or_string, AST):
        node_or_string = gast_to_ast(node_or_string)
    return _ast.literal_eval(node_or_string)


def get_docstring(node, clean=True):
    if not isinstance(node, (FunctionDef, ClassDef, Module)):
        raise TypeError("%r can't have docstrings" % node.__class__.__name__)
    if node.body and isinstance(node.body[0], Expr) and \
       isinstance(node.body[0].value, Str):
        if clean:
            import inspect
            return inspect.cleandoc(node.body[0].value.s)
        return node.body[0].value.s
