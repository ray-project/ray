from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI

# NumPy's RandomState/seed range is [0, 2**32 - 1].
NUMPY_RNG_SEED_MAX = 2**32


@dataclass(frozen=True)
class SeedTuple:
    """A hierarchical seed for ``np.random.default_rng()``.

    Levels are stored internally from least-variable (base seed) to
    most-variable (e.g., task index, compaction index).  When materialized
    via :meth:`as_rng_seed`, the order is reversed so the most-variable
    index appears first, matching NumPy's ``SeedSequence`` convention for
    optimal entropy mixing.

    Use :meth:`create` to start a new seed hierarchy, and :meth:`spawn` to
    derive child seeds at each successive level.

    Example::

        >>> import numpy as np
        >>> execution_idx, task_idx = 0, 1
        >>> seed = SeedTuple.create(42).spawn(execution_idx).spawn(task_idx)
        >>> rng = np.random.default_rng(seed.as_rng_seed())
    """

    _levels: tuple[int, ...]

    @staticmethod
    def create(base_seed: int) -> "SeedTuple":
        """Create a root seed with just the base value."""
        return SeedTuple(_levels=(base_seed,))

    def spawn(self, index: int) -> "SeedTuple":
        """Derive a child seed by appending a more-variable level."""
        return SeedTuple(_levels=(*self._levels, index))

    def as_rng_seed(self) -> tuple[int, ...]:
        """Materialize for ``np.random.default_rng()`` (most-variable first)."""
        return self._levels[::-1]


@DeveloperAPI
@dataclass
class RandomSeedConfig:
    """This configuration object controls the random seed behavior for operations
    such as :meth:`~Dataset.random_shuffle`, :meth:`~Dataset.randomize_block_order`,
    :meth:`~Dataset.random_sample`, and local shuffling in :meth:`~Dataset.iter_batches`
    (and related iterators). The random seed behavior is determined by
    the combination of the base seed ``seed`` and the ``reseed_after_execution`` parameter:

    - If ``seed`` is None, the random seed is always None (non-deterministic shuffling).
    - If ``seed`` is not None and ``reseed_after_execution`` is False, the base seed is
      used as the random seed for each execution.
    - If ``seed`` is not None and ``reseed_after_execution`` is True, the base seed is
      combined with the (incremental) execution index ``execution_idx`` to produce a
      different random seed tuple for each execution.

    .. note::
        Even if you provided a seed, you might still observe a non-deterministic row
        order. This is because tasks are executed in parallel and their completion
        order might vary. If you need to preserve the order of rows, set
        ``DataContext.get_current().execution_options.preserve_order``.

    Args:
        seed: An optional integer base seed. If None, the operation is
            non-deterministic. If provided, the operation is deterministic based on the base
            seed and the ``reseed_after_execution`` parameter.
        reseed_after_execution: If True, the random seed considers both ``seed`` and
            ``execution_idx``, resulting in different shuffling orders across executions.
            If False, the base seed is used as the random seed for each execution, resulting in the same
            shuffling order across executions. Only takes effect when a base seed is provided.
            Defaults to True.
        use_timestamp_as_default: When enabled, it supports a legacy behavior that relies on
            the timestamp as the default seed. This parameter is only used when the base seed
            is None. Defaults to False. See ``get_single_integer_random_seed`` for more details.

    """  # noqa: E501

    seed: Optional[int] = None
    reseed_after_execution: bool = True
    use_timestamp_as_default: bool = False
    # Internal field set via create_with_split_index().
    # Used by iteration APIs to differentiate parallel workers/shards.
    _split_index: Optional[int] = None

    def __post_init__(self):
        """Ensure that the seed is either None or an integer."""
        if self.seed is not None and not isinstance(self.seed, int):
            raise ValueError("Seed must be an integer or None.")

    def make_base_seed(self) -> SeedTuple | None:
        """Create a :class:`SeedTuple` with the base seed and split index.

        Returns ``None`` when ``seed`` is ``None`` (non-deterministic mode).
        The split index is included when set (i.e., for iteration APIs
        with multiple workers/shards).
        """
        if self.seed is None:
            return None
        seed = SeedTuple.create(self.seed)
        if self._split_index is not None:
            seed = seed.spawn(self._split_index)
        return seed

    def apply_execution_idx(
        self,
        seed: SeedTuple,
        *,
        data_context: DataContext,
    ) -> SeedTuple:
        """Append ``execution_idx`` to *seed* if reseeding is enabled.

        When ``reseed_after_execution`` is ``False`` the seed is returned
        unchanged.

        Args:
            seed: The seed to extend.
            data_context: Used to read the current execution index.

        Returns:
            The (possibly extended) seed.
        """
        if self.reseed_after_execution:
            return seed.spawn(data_context._execution_idx)
        return seed

    def make_seed(
        self,
        *,
        data_context: DataContext,
    ) -> SeedTuple | None:
        """Convenience: ``make_base_seed()`` + ``apply_execution_idx()``.

        Use when no caller-specific levels need to sit between the base seed
        and the execution index (e.g., dataset-level operations).

        Args:
            data_context: Used to read the current execution index.

        Returns:
            A :class:`SeedTuple`, or ``None`` for non-deterministic behavior.
        """
        seed = self.make_base_seed()
        if seed is not None:
            seed = self.apply_execution_idx(seed, data_context=data_context)
        return seed

    @classmethod
    def create_seed_config(
        cls,
        seed: int | RandomSeedConfig | None,
        *,
        use_timestamp_as_default: bool = False,
        split_index: Optional[int] = None,
    ) -> RandomSeedConfig:
        """Create a ``RandomSeedConfig`` object from the ``seed`` argument in Ray Data public random APIs.

        This is a helper function that converts an integer seed into a ``RandomSeedConfig`` object. In this case,
        ``reseed_after_execution`` is set to False, which matches the standard behavior for a data pipeline
        (i.e., same seed across executions). To override this behavior, use a ``RandomSeedConfig`` object
        directly.

        Args:
            seed: This optional argument can be an integer or an existing ``RandomSeedConfig`` object.
            use_timestamp_as_default: If True, a timestamp-based seed is used when
                ``seed`` is None. This pins the seed at plan time so that task retries
                produce identical output. When ``seed`` is an existing
                ``RandomSeedConfig``, this value overrides its
                ``use_timestamp_as_default`` field.
            split_index: The index of this split/worker. Used by iteration APIs
                to differentiate parallel workers/shards. Defaults to None.

        Returns:
            A ``RandomSeedConfig`` object.
        """
        if seed is None or isinstance(seed, int):
            return RandomSeedConfig(
                seed=seed,
                reseed_after_execution=False,
                use_timestamp_as_default=use_timestamp_as_default,
                _split_index=split_index,
            )
        elif isinstance(seed, RandomSeedConfig):
            return cls(
                seed=seed.seed,
                reseed_after_execution=seed.reseed_after_execution,
                use_timestamp_as_default=use_timestamp_as_default,
                _split_index=split_index
                if split_index is not None
                else seed._split_index,
            )

        raise ValueError(f"Invalid seed type: {type(seed)}")

    @classmethod
    def create_with_split_index(
        cls,
        seed: int | RandomSeedConfig | None,
        split_index: int,
    ) -> RandomSeedConfig | None:
        """Create a ``RandomSeedConfig`` for iteration APIs with split index set.

        This is a convenience method for iteration APIs (``iter_batches``, etc.)
        that normalizes the seed and sets the split index in one step. Returns
        ``None`` when ``seed`` is ``None`` (no shuffling configured).

        Args:
            seed: An integer, ``RandomSeedConfig``, or None.
            split_index: The index of this split/worker.

        Returns:
            A ``RandomSeedConfig`` with split index set, or None if seed is None.
        """
        if seed is None:
            return None
        return cls.create_seed_config(seed, split_index=split_index)


def get_timestamp_seed() -> int:
    """Returns a timestamp-based seed."""
    return time.time_ns() % NUMPY_RNG_SEED_MAX


def get_single_integer_random_seed(
    seed_config: RandomSeedConfig,
    data_context: DataContext,
) -> Optional[int]:
    """Returns a single integer seed based on the ``RandomSeedConfig`` object. This is useful
    if the caller expects a single integer to seed the RNG. When the base seed is None,
    and ``seed_config.use_timestamp_as_default`` is True, a timestamp is used as the seed.
    Otherwise, the seed tuple is hashed to produce a single integer seed.

    Args:
        seed_config: The ``RandomSeedConfig`` object.
        data_context: The ``DataContext`` object used to generate the seed tuple.

    Returns:
        A single integer random seed, or None for non-deterministic behavior.
    """
    seed = seed_config.make_seed(data_context=data_context)

    if seed is None:
        # This is a legacy behavior for some random operations.
        return get_timestamp_seed() if seed_config.use_timestamp_as_default else None

    rng_seed = seed.as_rng_seed()
    if len(rng_seed) == 1:
        return rng_seed[0]

    # The modulo is only needed because some random implementations are using the
    # older type RandomState or np.random.seed(). Otherwise, the seed can be
    # as large as 128-bit integer. See
    # https://blog.scientific-python.org/numpy/numpy-rng/
    return hash(rng_seed) % NUMPY_RNG_SEED_MAX
