import logging
import os
import re
from functools import lru_cache
from typing import Iterable, Optional, Pattern, Tuple

logger = logging.getLogger(__name__)

RAY_METRICS_EXCLUDE_NAMES = "RAY_METRICS_EXCLUDE_NAMES"
RAY_METRICS_EXCLUDE_PATTERNS = "RAY_METRICS_EXCLUDE_PATTERNS"
_PATTERN_MATCH_CACHE_SIZE = 1024


def _make_pattern_matcher(patterns: Tuple[Pattern[str], ...]):
    @lru_cache(maxsize=_PATTERN_MATCH_CACHE_SIZE)
    def matches(name: str) -> bool:
        return any(pattern.fullmatch(name) for pattern in patterns)

    return matches


class MetricsExclusionConfig:
    """Configuration for excluding metric families from export.

    Names and patterns match internal metric names before Ray adds the ``ray_``
    Prometheus namespace prefix.
    """

    def __init__(
        self,
        exclude_names: Optional[Iterable[str]] = None,
        exclude_patterns: Optional[Iterable[str]] = None,
    ):
        self.exclude_names = frozenset(
            self._normalize(
                exclude_names
                if exclude_names is not None
                else self._parse_env_list(RAY_METRICS_EXCLUDE_NAMES)
            )
        )
        self.exclude_patterns = tuple(
            self._normalize(
                exclude_patterns
                if exclude_patterns is not None
                else self._parse_env_list(RAY_METRICS_EXCLUDE_PATTERNS)
            )
        )
        self._compiled_patterns = self._compile_patterns(self.exclude_patterns)
        self._matches_pattern = _make_pattern_matcher(self._compiled_patterns)

        if self.exclude_names or self.exclude_patterns:
            logger.info(
                "Metrics exclusion config initialized: names=%s, patterns=%s",
                sorted(self.exclude_names) or "none",
                list(self.exclude_patterns) or "none",
            )

    def is_excluded(self, name: str) -> bool:
        if not self.exclude_names and not self._compiled_patterns:
            return False
        if name in self.exclude_names:
            return True
        if not self._compiled_patterns:
            return False
        return self._matches_pattern(name)

    @staticmethod
    def _normalize(values: Iterable[str]) -> Iterable[str]:
        return (value.strip() for value in values if value.strip())

    @staticmethod
    def _parse_env_list(env_var: str) -> Iterable[str]:
        return os.environ.get(env_var, "").split(",")

    @staticmethod
    def _compile_patterns(patterns: Iterable[str]) -> Tuple[Pattern[str], ...]:
        compiled_patterns = []
        for pattern in patterns:
            try:
                compiled_patterns.append(re.compile(pattern))
            except re.error as error:
                logger.error(
                    "Ignoring invalid metric exclusion pattern %r from %s: %s",
                    pattern,
                    RAY_METRICS_EXCLUDE_PATTERNS,
                    error,
                )
        return tuple(compiled_patterns)
