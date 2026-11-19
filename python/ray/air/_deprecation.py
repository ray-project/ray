"""Shared settings for the deprecated `ray.air` import paths (removed in Ray 3.0)."""

from typing import Tuple

from ray._common.deprecation import DeprecatedAlias

REMOVAL_VERSION = "3.0"
MIGRATION_NOTE = (
    "See this issue for more context and migration options: "
    "https://github.com/ray-project/ray/issues/49454."
)


def alias(target: str, *replacements: Tuple[str, str]) -> DeprecatedAlias:
    return DeprecatedAlias(target, replacements, note=MIGRATION_NOTE)


def tune_or_train(name: str) -> Tuple[Tuple[str, str], ...]:
    return (
        (f"ray.tune.{name}", "with Ray Tune"),
        (f"ray.train.{name}", "with Ray Train"),
    )
