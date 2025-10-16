import ast
import logging

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from ray.data.expressions import ColumnExpr, Expr

logger = logging.getLogger(__name__)


# NOTE: (srinathk) There are 3 distinct stages of handling passed in exprs:
# 1. Parsing it (as text)
# 2. Resolving unbound names (to schema)
# 3. Converting resolved expressions to PA ones
# Need to break up the abstraction provided by ExpressionEvaluator.


class ExpressionEvaluator:
    @staticmethod
    def get_filters(expression: str) -> ds.Expression:
        """Parse and evaluate the expression to generate a filter condition.

        Args:
            expression: A string representing the filter expression to parse.

        Returns:
            A PyArrow compute expression for filtering data.

        """
        try:
            tree = ast.parse(expression, mode="eval")
            return _ConvertToArrowExpressionVisitor().visit(tree.body)
        except SyntaxError as e:
            raise ValueError(f"Invalid syntax in the expression: {expression}") from e
        except Exception as e:
            logger.exception(f"Error processing expression: {e}")
            raise

    @staticmethod
    def parse_native_expression(expression: str) -> "Expr":
        """Parse and evaluate the expression to generate a Ray Data expression.

        Args:
            expression: A string representing the filter expression to parse.

        Returns:
            A Ray Data Expr object for filtering data.

        """
        try:
            tree = ast.parse(expression, mode="eval")
            return _ConvertToNativeExpressionVisitor().visit(tree.body)
        except SyntaxError as e:
            raise ValueError(f"Invalid syntax in the expression: {expression}") from e
        except Exception as e:
            logger.exception(f"Error processing expression: {e}")
            raise


class _ConvertToArrowExpressionVisitor(ast.NodeVisitor):
    # TODO: Deprecate this visitor after we remove string support in filter API.
    def visit_Compare(self, node: ast.Compare) -> ds.Expression:
        """Handle comparison operations (e.g., a == b, a < b, a in b).

        Args:
            node: The AST node representing a comparison operation.

        Returns:
            An expression representing the comparison.
        """
        # Handle left operand
        # TODO Validate columns
        if isinstance(node.left, ast.Attribute):
            # Visit and handle attributes
            left_expr = self.visit(node.left)
        elif isinstance(node.left, ast.Name):
            # Treat as a simple field
            left_expr = self.visit(node.left)
        elif isinstance(node.left, ast.Constant):
            # Constant values are used directly
            left_expr = node.left.value
        else:
            raise ValueError(f"Unsupported left operand type: {type(node.left)}")

        comparators = [self.visit(comp) for comp in node.comparators]

        op = node.ops[0]
        if isinstance(op, ast.In):
            return left_expr.is_in(comparators[0])
        elif isinstance(op, ast.NotIn):
            return ~left_expr.is_in(comparators[0])
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

    def visit_BoolOp(self, node: ast.BoolOp) -> ds.Expression:
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

    def visit_Name(self, node: ast.Name) -> ds.Expression:
        """Handle variable (name) nodes and return them as pa.dataset.Expression.

        Even if the name contains periods, it's treated as a single string.

        Args:
            node: The AST node representing a variable.

        Returns:
            The variable wrapped as a pa.dataset.Expression.
        """
        # Directly use the field name as a string (even if it contains periods)
        field_name = node.id
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

    def visit_List(self, node: ast.List) -> ds.Expression:
        """Handle list literals.

        Args:
            node: The AST node representing a list.

        Returns:
            The list of elements wrapped as a pa.dataset.Expression.
        """
        elements = [self.visit(elt) for elt in node.elts]
        return pa.array(elements)

    def visit_UnaryOp(self, node: ast.UnaryOp) -> ds.Expression:
        """Handle case where comparator is UnaryOP (e.g., a == -1).
        AST for this expression will be Compare(left=Name(id='a'), ops=[Eq()],
        comparators=[UnaryOp(op=USub(), operand=Constant(value=1))])

        Args:
            node: The constant value."""

        op = node.op
        if isinstance(op, ast.USub):
            return pc.scalar(-node.operand.value)
        else:
            raise ValueError(f"Unsupported unary operator: {op}")

    # TODO (srinathk) Note that visit_Constant does not return pa.dataset.Expression
    # because to support function in() which takes in a List, the elements in the List
    # needs to values instead of pa.dataset.Expression per pyarrow.dataset.Expression
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

    def visit_Call(self, node: ast.Call) -> ds.Expression:
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
            "is_in": lambda arg1, arg2: arg1.is_in(arg2),
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
            # Handle the "is_in" function with exactly two arguments
            elif func_name == "is_in" and len(args) != 2:
                raise ValueError("is_in function requires two arguments.")
            # Ensure the function has one argument (for functions like is_valid)
            elif func_name != "is_in" and len(args) != 1:
                raise ValueError(f"{func_name} function requires exactly one argument.")
            # Call the corresponding function with the arguments
            return function_map[func_name](*args)
        else:
            raise ValueError(f"Unsupported function: {func_name}")


class _ConvertToNativeExpressionVisitor(ast.NodeVisitor):
    """AST visitor that converts string expressions to Ray Data expressions."""

    def visit_Compare(self, node: ast.Compare) -> "Expr":
        """Handle comparison operations (e.g., a == b, a < b, a in b)."""
        from ray.data.expressions import BinaryExpr, Operation

        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise ValueError("Only simple binary comparisons are supported")

        left = self.visit(node.left)
        right = self.visit(node.comparators[0])
        op = node.ops[0]

        # Map AST comparison operators to Ray Data operations
        op_map = {
            ast.Eq: Operation.EQ,
            ast.NotEq: Operation.NE,
            ast.Lt: Operation.LT,
            ast.LtE: Operation.LE,
            ast.Gt: Operation.GT,
            ast.GtE: Operation.GE,
            ast.In: Operation.IN,
            ast.NotIn: Operation.NOT_IN,
        }

        if type(op) not in op_map:
            raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")

        return BinaryExpr(op_map[type(op)], left, right)

    def visit_BoolOp(self, node: ast.BoolOp) -> "Expr":
        """Handle logical operations (e.g., a and b, a or b)."""
        from ray.data.expressions import BinaryExpr, Operation

        conditions = [self.visit(value) for value in node.values]
        combined_expr = conditions[0]

        for condition in conditions[1:]:
            if isinstance(node.op, ast.And):
                combined_expr = BinaryExpr(Operation.AND, combined_expr, condition)
            elif isinstance(node.op, ast.Or):
                combined_expr = BinaryExpr(Operation.OR, combined_expr, condition)
            else:
                raise ValueError(
                    f"Unsupported logical operator: {type(node.op).__name__}"
                )

        return combined_expr

    def visit_UnaryOp(self, node: ast.UnaryOp) -> "Expr":
        """Handle unary operations (e.g., not a, -5)."""
        from ray.data.expressions import Operation, UnaryExpr, lit

        if isinstance(node.op, ast.Not):
            operand = self.visit(node.operand)
            return UnaryExpr(Operation.NOT, operand)
        elif isinstance(node.op, ast.USub):
            operand = self.visit(node.operand)
            return operand * lit(-1)
        else:
            raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")

    def visit_Name(self, node: ast.Name) -> "Expr":
        """Handle variable names (column references)."""
        from ray.data.expressions import col

        return col(node.id)

    def visit_Constant(self, node: ast.Constant) -> "Expr":
        """Handle constant values (numbers, strings, booleans)."""
        from ray.data.expressions import lit

        return lit(node.value)

    def visit_List(self, node: ast.List) -> "Expr":
        """Handle list literals."""
        from ray.data.expressions import LiteralExpr, lit

        # Visit all elements first
        visited_elements = [self.visit(elt) for elt in node.elts]

        # Try to extract constant values for literal list
        elements = []
        for elem in visited_elements:
            if isinstance(elem, LiteralExpr):
                elements.append(elem.value)
            else:
                # For compatibility with Arrow visitor, we need to support non-literals
                # but Ray Data expressions may have limitations here
                raise ValueError(
                    "List contains non-constant expressions. Ray Data expressions "
                    "currently only support lists of constant values."
                )

        return lit(elements)

    def visit_Attribute(self, node: ast.Attribute) -> "Expr":
        """Handle attribute access (e.g., for nested column names)."""
        from ray.data.expressions import col

        # For nested column names like "user.age", combine them with dots
        if isinstance(node.value, ast.Name):
            return col(f"{node.value.id}.{node.attr}")
        elif isinstance(node.value, ast.Attribute):
            # Recursively handle nested attributes
            left_expr = self.visit(node.value)
            if isinstance(left_expr, ColumnExpr):
                return col(f"{left_expr._name}.{node.attr}")

        raise ValueError(
            f"Unsupported attribute access: {node.attr}. Node details: {ast.dump(node)}"
        )

    def visit_Call(self, node: ast.Call) -> "Expr":
        """Handle function calls for operations like is_null, is_not_null, is_nan."""
        from ray.data.expressions import BinaryExpr, Operation, UnaryExpr

        func_name = node.func.id if isinstance(node.func, ast.Name) else str(node.func)

        if func_name == "is_null":
            if len(node.args) != 1:
                raise ValueError("is_null() expects exactly one argument")
            operand = self.visit(node.args[0])
            return UnaryExpr(Operation.IS_NULL, operand)
        # Adding this conditional to keep it consistent with the current implementation,
        # of carrying Pyarrow's semantic of `is_valid`
        elif func_name == "is_valid" or func_name == "is_not_null":
            if len(node.args) != 1:
                raise ValueError(f"{func_name}() expects exactly one argument")
            operand = self.visit(node.args[0])
            return UnaryExpr(Operation.IS_NOT_NULL, operand)
        elif func_name == "is_nan":
            if len(node.args) != 1:
                raise ValueError("is_nan() expects exactly one argument")
            operand = self.visit(node.args[0])
            # Use x != x pattern for NaN detection (NaN != NaN is True)
            return BinaryExpr(Operation.NE, operand, operand)
        elif func_name == "is_in":
            if len(node.args) != 2:
                raise ValueError("is_in() expects exactly two arguments")
            left = self.visit(node.args[0])
            right = self.visit(node.args[1])
            return BinaryExpr(Operation.IN, left, right)
        else:
            raise ValueError(f"Unsupported function: {func_name}")
