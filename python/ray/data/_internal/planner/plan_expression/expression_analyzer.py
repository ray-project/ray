from __future__ import annotations

import copy as cp
from typing import TYPE_CHECKING, List, Set, Type

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Filter, Project
from ray.data._internal.logical.ruleset import Ruleset
from ray.data.datatype import DataType
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

if TYPE_CHECKING:
    from ray.data import Schema


class ResolveAttributes(Rule):
    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        old_dag = plan.dag
        new_dag = old_dag._apply_transform(self.resolve)
        return LogicalPlan(new_dag, plan.context) if old_dag is not new_dag else plan

    def resolve(self, op: LogicalOperator) -> LogicalOperator:
        op_schema = op.infer_schema()
        if op_schema is None:
            return op

        match op:
            case Project(exprs=exprs):
                new_exprs: List[Expr] = []
                for expr in exprs:
                    new_expr = self.resolve_attributes(expr=expr, schema=op_schema)
                    new_exprs.append(new_expr)
                new_op = cp.copy(op)
                new_op._exprs = new_exprs
                return new_op

            case Filter(_predicate_expr=expr) if expr is not None:
                new_expr = self.resolve_attributes(expr=expr, schema=op_schema)
                new_op = cp.copy(op)
                new_op._predicate_expr = new_expr
                return new_op

        return op

    # TODO(Justin): Change this to the visitor model.
    def resolve_attributes(self, expr: Expr, schema: Schema) -> Expr:
        match expr:
            case UnresolvedColumnExpr(_name=name):
                if name not in schema.names:
                    raise ValueError(f"Column name {name} not in schema: {schema}")
                index = schema.names.index(name)
                raw_type = schema.types[index]
                # TODO(Justin): Could be pandas schema?
                data_type = DataType.from_arrow(raw_type)
                return ResolvedColumnExpr(_name=name, _data_type=data_type)
            case BinaryExpr(op=op, left=left, right=right):
                new_left = self.resolve_attributes(left, schema)
                new_right = self.resolve_attributes(right, schema)
                return BinaryExpr(op=op, left=new_left, right=new_right)

            case UnaryExpr(op=op, operand=operand):
                new_operand = self.resolve_attributes(operand, schema)
                return UnaryExpr(op=op, operand=new_operand)

            case AliasExpr(expr=child, _name=name, _is_rename=is_rename):
                new_child = self.resolve_attributes(child, schema)
                return AliasExpr(expr=new_child, _name=name, _is_rename=is_rename)

            case UDFExpr(fn=fn, args=args, kwargs=kwargs, data_type=dtype):
                new_args = [self.resolve_attributes(arg, schema) for arg in args]
                new_kwargs = {
                    k: self.resolve_attributes(v, schema) for k, v in kwargs.items()
                }
                return UDFExpr(
                    fn=fn, args=new_args, kwargs=new_kwargs, _data_type=dtype
                )

            case DownloadExpr(uri_column=uri_column):
                new_uri = self.resolve_attributes(uri_column, schema)
                return DownloadExpr(uri_column=new_uri)

        return expr


class ResolveStar(Rule):
    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        old_dag = plan.dag
        new_dag = old_dag._apply_transform(self.resolve)
        return LogicalPlan(new_dag, plan.context) if old_dag is not new_dag else plan

    def resolve(self, op: LogicalOperator) -> LogicalOperator:
        op_schema = op.infer_schema()
        if op_schema is None:
            return op

        match op:
            case Project(exprs=exprs) if op.has_star_expr():
                existing_cols: Set[str] = set()
                existing_exprs: List[Expr] = []
                additional_exprs: List[Expr] = []
                for expr in exprs:
                    if isinstance(expr, StarExpr):
                        # skip star expressions
                        pass
                    elif isinstance(expr, AliasExpr) and not expr._is_rename:
                        # Additional column
                        additional_exprs.append(expr)
                    else:
                        # Existing column
                        existing_cols.add(expr.get_root_name())
                        existing_exprs.append(expr)

                assert len(existing_cols) == len(existing_exprs)

                non_existing_exprs: List[Expr] = []
                for i, col_name in enumerate(op_schema.names):
                    if col_name not in existing_cols:
                        raw_type = op_schema.types[i]
                        # TODO(Justin): Could be pandas schema?
                        data_type = DataType.from_arrow(raw_type)
                        non_existing_exprs.append(
                            ResolvedColumnExpr(_name=col_name, _data_type=data_type)
                        )

                op = cp.copy(op)
                op._exprs = existing_exprs + non_existing_exprs + additional_exprs
                return op

            # TODO(Justin): Can filter contain stars?
        return op

    @classmethod
    def dependencies(cls) -> List[Type["Rule"]]:
        """We have to resolve the attributes first before applying star expansion
        because we do not know ahead of time what additional columns are needed."""
        return [ResolveAttributes]


_RULES = Ruleset([ResolveAttributes, ResolveStar])


class Analyzer:
    """Resolves expressions using rules"""

    @classmethod
    def analyze(cls, plan: LogicalPlan) -> LogicalPlan:
        while True:
            curr_plan = plan
            for rule in _RULES:
                curr_plan = rule().apply(curr_plan)

            if curr_plan is plan:
                break
            plan = curr_plan
        return plan
