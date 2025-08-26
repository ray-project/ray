"""Base SQL validator class for Ray Data SQL API."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

from ray.data.sql.exceptions import (
    SQLParseError,
    UnsupportedOperationError,
)


class SQLValidator(ABC):
    """Abstract base class for SQL validators.

    This class defines the interface that all SQL validators must implement.
    Validators are responsible for ensuring SQL queries meet specific criteria
    such as feature support, syntax correctness, and semantic validity.
    """

    @abstractmethod
    def validate(self, query: str, ast: exp.Expression) -> None:
        """Validate a SQL query.

        Args:
            query: Original SQL query string.
            ast: Parsed SQLGlot AST.

        Raises:
            SQLParseError: If the query has syntax or semantic errors.
            UnsupportedOperationError: If the query uses unsupported features.
        """
        pass

    @abstractmethod
    def get_supported_features(self) -> Set[str]:
        """Get a set of supported feature names.

        Returns:
            Set of feature names that this validator supports.
        """
        pass

    @abstractmethod
    def get_unsupported_features(self) -> Set[str]:
        """Get a set of unsupported feature names.

        Returns:
            Set of feature names that this validator does not support.
        """
        pass


class CompositeValidator(SQLValidator):
    """Composite validator that combines multiple validators.

    This validator runs multiple validation passes, stopping at the first
    validation failure.
    """

    def __init__(self, validators: List[SQLValidator]):
        """Initialize with a list of validators.

        Args:
            validators: List of validators to run in sequence.
        """
        self.validators = validators

    def validate(self, query: str, ast: exp.Expression) -> None:
        """Run all validators in sequence.

        Args:
            query: Original SQL query string.
            ast: Parsed SQLGlot AST.
        """
        for validator in self.validators:
            validator.validate(query, ast)

    def get_supported_features(self) -> Set[str]:
        """Get intersection of all supported features.

        Returns:
            Set of features supported by all validators.
        """
        if not self.validators:
            return set()

        features = self.validators[0].get_supported_features()
        for validator in self.validators[1:]:
            features &= validator.get_supported_features()
        return features

    def get_unsupported_features(self) -> Set[str]:
        """Get union of all unsupported features.

        Returns:
            Set of features unsupported by any validator.
        """
        features = set()
        for validator in self.validators:
            features |= validator.get_unsupported_features()
        return features
