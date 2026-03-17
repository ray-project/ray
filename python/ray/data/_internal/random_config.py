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
    """A seed for random number generation, optionally including execution index for reseeding.

    Args:
        seed: The base seed.
        execution_idx: The execution index. If None, the seed is not reseeded after execution.

    """

    seed: int
    execution_idx: Optional[int] = None

    def to_rng_args(self, task_idx: int) -> tuple[int, ...]:
        """Return seed parts for np.random.default_rng((task_idx, ...)).

        Args:
            task_idx: The task index.

        Returns:
            A tuple of seed parts.
        """
        if self.execution_idx is None:
            return (task_idx, self.seed)
        return (task_idx, self.execution_idx, self.seed)


@DeveloperAPI
@dataclass
class RandomSeedConfig:
    """This configuration object controls the random seed behavior for operations
    such as :meth:`~Dataset.random_shuffle`, :meth:`~Dataset.randomize_block_order`,
    and :meth:`~Dataset.random_sample`. The random seed behavior is determined by
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

    def __post_init__(self):
        """Ensure that the seed is either None or an integer."""
        if self.seed is not None and not isinstance(self.seed, int):
            raise ValueError("Seed must be an integer or None.")

    def get_seed_tuple(
        self,
        *,
        data_context: DataContext,
    ) -> SeedTuple | None:
        """Return a seed for random number generation.

        Args:
            data_context: A DataContext object for extracting the
                execution index.

        Returns:
            A SeedTuple, or None for non-deterministic behavior.
        """
        if self.seed is None:
            return None
        elif self.reseed_after_execution:
            return SeedTuple(
                seed=self.seed,
                execution_idx=data_context._execution_idx,
            )
        else:
            return SeedTuple(seed=self.seed, execution_idx=None)

    @classmethod
    def create_seed_config(
        cls,
        seed: int | RandomSeedConfig | None,
        *,
        use_timestamp_as_default: bool = False,
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

        Returns:
            A ``RandomSeedConfig`` object.
        """
        if seed is None or isinstance(seed, int):
            return RandomSeedConfig(
                seed=seed,
                reseed_after_execution=False,
                use_timestamp_as_default=use_timestamp_as_default,
            )
        elif isinstance(seed, RandomSeedConfig):
            seed.use_timestamp_as_default = use_timestamp_as_default
            return seed

        raise ValueError(f"Invalid seed type: {type(seed)}")


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
    seed_result = seed_config.get_seed_tuple(data_context=data_context)

    if seed_result is None:
        # This is a legacy behavior for some random operations.
        return get_timestamp_seed() if seed_config.use_timestamp_as_default else None
    elif seed_result.execution_idx is None:
        return seed_result.seed

    # The modulo is only needed because some random implementations are using the
    # older type RandomState or np.random.seed(). Otherwise, the seed can be
    # as large as 128-bit integer. See
    # https://blog.scientific-python.org/numpy/numpy-rng/
    return hash(seed_result) % NUMPY_RNG_SEED_MAX
