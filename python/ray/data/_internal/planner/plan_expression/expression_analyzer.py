import abc

from ray.data import Schema
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    DownloadExpr,
    Expr,
    ResolvedColumnExpr,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    UnresolvedColumnExpr,
)


class Rule(abc.ABC):
    @classmethod
    def apply(cls, expr: Expr, schema: Schema) -> Expr:
        ...


# TODO(Justin): Change this to the visitor model.
class ResolveAttributes(Rule):
    @classmethod
    def apply(cls, expr: Expr, schema: Schema) -> Expr:
        match expr:
            case UnresolvedColumnExpr(_name=name):
                if name not in schema.names:
                    raise ValueError(f"Column name {name} not in schema: {schema}")
                index = schema.names.index(name)
                return ResolvedColumnExpr(_name=name, _data_type=schema.types[index])
            case BinaryExpr(op=op, left=left, right=right):
                new_left = cls.apply(left, schema)
                new_right = cls.apply(right, schema)
                return BinaryExpr(op=op, left=new_left, right=new_right)

            case UnaryExpr(op=op, operand=operand):
                new_operand = cls.apply(operand, schema)
                return UnaryExpr(op=op, operand=new_operand)

            case AliasExpr(expr=child, _name=name, _is_rename=is_rename):
                new_child = cls.apply(child, schema)
                return AliasExpr(expr=new_child, _name=name, _is_rename=is_rename)

            case UDFExpr(fn=fn, args=args, kwargs=kwargs, data_type=dtype):
                new_args = [cls.apply(arg, schema) for arg in args]
                new_kwargs = {k: cls.apply(v, schema) for k, v in kwargs.items()}
                return UDFExpr(fn=fn, args=new_args, kwargs=new_kwargs, data_type=dtype)

            case DownloadExpr(uri_column=uri_column):
                new_uri = cls.apply(uri_column, schema)
                return DownloadExpr(uri_column=new_uri)

        return expr


class ResolveStar(Rule):
    @classmethod
    def apply(cls, expr: Expr, schema: Schema) -> Expr:
        match expr:
            case StarExpr():
                return StarExpr()

        return expr


_RULES = [ResolveAttributes]


class Analyzer:
    """Resolves expressions using rules"""

    @classmethod
    def analyze(cls, expr: Expr, schema: Schema) -> Expr:
        while True:
            new_expr = expr
            for rule in _RULES:
                new_expr = rule.apply(new_expr, schema)

            if new_expr.structurally_equals(expr):
                break
            expr = new_expr
        return expr
