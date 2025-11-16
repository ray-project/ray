"""
Data types and structures for SQL execution.

This module contains shared data classes and types used across the SQL execution layer.
"""

from dataclasses import dataclass
from typing import Optional, Tuple

from ray.data import Dataset


@dataclass
class JoinInfo:
    """Information about a JOIN operation following Ray Data Join API.

    This dataclass stores join information that maps to the Ray Data Join API
    parameters: join_type, on, right_on, left_suffix, right_suffix, num_partitions.

    Args:
        left_table: Name of the left table.
        right_table: Name of the right table.
        left_columns: Join columns in the left table (as tuple for API compatibility).
        right_columns: Join columns in the right table (as tuple for API compatibility).
        join_type: Type of join (inner, left_outer, right_outer, full_outer).
        left_dataset: The left Ray Dataset.
        right_dataset: The right Ray Dataset.
        left_suffix: Suffix for left operand columns (default: "").
        right_suffix: Suffix for right operand columns (default: "_r").
        num_partitions: Number of partitions for join operation.
    """

    left_table: str
    right_table: str
    left_columns: Tuple[str, ...]  # Changed to tuple for API compatibility
    right_columns: Tuple[str, ...]  # Changed to tuple for API compatibility
    join_type: str
    left_dataset: Optional[Dataset] = None
    right_dataset: Optional[Dataset] = None
    left_suffix: str = ""
    right_suffix: str = "_r"
    num_partitions: int = 10
