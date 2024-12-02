import ast
import logging
from typing import Any, List, Union

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)


# NOTE: (srinathk) There are 3 distinct stages of handling passed in exprs:
# 1. Parsing it (as text)
# 2. Resolving unbound names (to schema)
# 3. Converting resolved expressions to PA ones
# Need to break up the abstraction provided by ExpressionEvaluator.


class ExpressionEvaluator:
    def get_filters(self, expression: str) -> pc.Expression:
        """Parse and evaluate the expression to generate a filter condition.

        Args:
            expression: A string representing the filter expression to parse.

        Returns:
            A PyArrow compute expression for filtering data.

        """
        try:
            tree = ast.parse(expression, mode="eval")
            return self._build_filter_condition(tree.body)
        except SyntaxError as e:
            raise ValueError(f"Invalid syntax in the expression: {expression}") from e
        except Exception as e:
            logger.exception(f"Error processing expression: {e}")
            raise

    def _build_filter_condition(self, node) -> Union[pc.Expression, List[Any], str]:
        """Recursively evaluate an AST node to build the filter condition.

        Args:
            node: The AST node to evaluate, representing part of the expression.

        Returns:
            The evaluated result for the node, which could be a
                filter condition, list, or field name.
        """
        visitor = _ConvertToArrowExpressionVisitor()
        return visitor.visit(node)


class _ConvertToArrowExpressionVisitor(ast.NodeVisitor):
    def visit_Compare(self, node: ast.Compare) -> pc.Expression:
        """Handle comparison operations (e.g., a == b, a < b, a in b).

        Args:
            node: The AST node representing a comparison operation.

        Returns:
            An expression representing the comparison.
        """
        # Handle left operand
        # TODO Validate columns
        if isinstance(node.left, ast.Attribute):
            left_expr = self.visit(node.left)  # Visit and handle attributes
        elif isinstance(node.left, ast.Name):
            left_expr = self.visit(node.left)  # Treat as a simple field
        elif isinstance(node.left, ast.Constant):
            left_expr = node.left.value  # Constant values are used directly
        else:
            raise ValueError(f"Unsupported left operand type: {type(node.left)}")

        comparators = [self.visit(comp) for comp in node.comparators]

        op = node.ops[0]
        if isinstance(op, ast.In):
            return left_expr.isin(comparators[0])
        elif isinstance(op, ast.NotIn):
            return ~left_expr.isin(comparators[0])
        elif isinstance(op, ast.Eq):
            return left_expr == comparators[0]
        elif isinstance(op, ast.NotEq):
            return left_expr != comparators[0]
        elif isinstance(op, ast.Lt):
            return left_expr < comparators[0]
        elif isinstance(op, ast.LtE):
            return left_expr <= comparators[0]
        elif isinstance(op, ast.Gt):
            return left_expr > comparators[0]
        elif isinstance(op, ast.GtE):
            return left_expr >= comparators[0]
        else:
            raise ValueError(f"Unsupported operator type: {op}")

    def visit_BoolOp(self, node: ast.BoolOp) -> pc.Expression:
        """Handle logical operations (e.g., a and b, a or b).

        Args:
            node: The AST node representing a boolean operation.

        Returns:
            An expression representing the logical operation.
        """
        conditions = [self.visit(value) for value in node.values]
        combined_expr = conditions[0]

        for condition in conditions[1:]:
            if isinstance(node.op, ast.And):
                # Combine conditions with logical AND
                combined_expr &= condition
            elif isinstance(node.op, ast.Or):
                # Combine conditions with logical OR
                combined_expr |= condition
            else:
                raise ValueError(
                    f"Unsupported logical operator: {type(node.op).__name__}"
                )

        return combined_expr

    def visit_Name(self, node: ast.Name) -> pc.Expression:
        """Handle variable (name) nodes and return them as pc.Expression.

        Even if the name contains periods, it's treated as a single string.

        Args:
            node: The AST node representing a variable.

        Returns:
            The variable wrapped as a pc.Expression.
        """
        field_name = (
            node.id
        )  # Directly use the field name as a string (even if it contains periods)
        return pc.field(field_name)

    def visit_Attribute(self, node: ast.Attribute) -> object:
        """Handle attribute access (e.g., np.nan).

        Args:
            node: The AST node representing an attribute access.

        Returns:
            object: The attribute value.

        Raises:
            ValueError: If the attribute is unsupported.
        """
        # Recursively visit the left side (base object or previous attribute)
        if isinstance(node.value, ast.Attribute):
            # If the value is an attribute, recursively resolve it
            left_expr = self.visit(node.value)
            return pc.field(f"{left_expr}.{node.attr}")

        elif isinstance(node.value, ast.Name):
            # If the value is a name (e.g., "foo"), we can directly return the field
            left_name = node.value.id  # The base name, e.g., "foo"
            return pc.field(f"{left_name}.{node.attr}")

        raise ValueError(f"Unsupported attribute: {node.attr}")

    def visit_List(self, node: ast.List) -> pc.Expression:
        """Handle list literals.

        Args:
            node: The AST node representing a list.

        Returns:
            The list of elements wrapped as a pc.Expression.
        """
        elements = [self.visit(elt) for elt in node.elts]
        return pa.array(elements)

    # TODO (srinathk) Note that visit_Constant does not return pc.Expression
    # because to support function in() which takes in a List, the elements in the List
    # needs to values instead of pc.Expression per pyarrow.dataset.Expression
    # specification. May be down the road, we can update it as Arrow relaxes this
    # constraint.
    def visit_Constant(self, node: ast.Constant) -> object:
        """Handle constant values (e.g., numbers, strings).

        Args:
            node: The AST node representing a constant value.

        Returns:
            object: The constant value itself (e.g., number, string, or boolean).
        """
        return node.value  # Return the constant value directly.

    def visit_Call(self, node: ast.Call) -> pc.Expression:
        """Handle function calls (e.g., is_nan(a), is_valid(b)).

        Args:
            node: The AST node representing a function call.

        Returns:
            The corresponding expression based on the function called.

        Raises:
            ValueError: If the function is unsupported or has incorrect arguments.
        """
        func_name = node.func.id
        function_map = {
            "is_nan": lambda arg: arg.is_nan(),
            "is_null": lambda arg, nan_is_null=False: arg.is_null(
                nan_is_null=nan_is_null
            ),
            "is_valid": lambda arg: arg.is_valid(),
            "isin": lambda arg1, arg2: arg1.isin(arg2),
        }

        if func_name in function_map:
            # Visit all arguments of the function call
            args = [self.visit(arg) for arg in node.args]
            # Handle the "is_null" function with one or two arguments
            if func_name == "is_null":
                if len(args) == 1:
                    return function_map[func_name](args[0])
                elif len(args) == 2:
                    return function_map[func_name](args[0], args[1])
                else:
                    raise ValueError("is_null function requires one or two arguments.")
            # Handle the "isin" function with exactly two arguments
            elif func_name == "isin" and len(args) != 2:
                raise ValueError("isin function requires two arguments.")
            # Ensure the function has one argument (for functions like is_valid)
            elif func_name != "isin" and len(args) != 1:
                raise ValueError(f"{func_name} function requires exactly one argument.")
            # Call the corresponding function with the arguments
            return function_map[func_name](*args)
        else:
            raise ValueError(f"Unsupported function: {func_name}")
