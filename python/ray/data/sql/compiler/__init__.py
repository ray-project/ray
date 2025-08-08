"""
Expression compilation module for Ray Data SQL API.

This module provides the critical bridge between SQL expressions and Ray Dataset
operations by compiling SQLGlot AST expressions into Python callables that can
be efficiently executed on distributed data.

Key Components:
- ExpressionCompiler: Main compiler that converts SQL expressions to Python functions

The compilation process transforms SQL expressions into Python lambda functions
that operate on row dictionaries, enabling:

1. Column References: Access to row data by column name (e.g., "name", "users.id")
2. Arithmetic Operations: Mathematical expressions (+, -, *, /, %) with proper null handling
3. Comparison Operations: Relational comparisons (=, <>, <, >, <=, >=) with type coercion
4. Logical Operations: Boolean logic (AND, OR, NOT) with SQL three-value logic
5. String Functions: Text operations (UPPER, LOWER, LENGTH, SUBSTRING, etc.)
6. Conditional Logic: CASE WHEN expressions for complex conditional evaluation
7. Null Handling: Proper SQL null semantics (IS NULL, IS NOT NULL)

The compiled functions are designed for efficient distributed execution in Ray,
handling type coercion, null propagation, and error cases according to SQL standards.
"""

from ray.data.sql.compiler.expressions import ExpressionCompiler

__all__ = [
    "ExpressionCompiler",
]
