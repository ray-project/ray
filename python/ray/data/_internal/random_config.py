from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI
from ray.util.common import INT32_MAX


@DeveloperAPI
@dataclass
class RandomSeedConfig:
    """This configuration object controls the random seed behavior for operations
    like :meth:`~Dataset.random_shuffle`, :meth:`~Dataset.randomize_block_order`,
    and :meth:`~Dataset.random_sample`. The random seed behavior is determined by
    the combination of ``seed`` and ``reseed_after_execution``:

    - If ``seed`` is None, the random seed is always None (non-deterministic shuffling).
    - If ``seed`` is not None and ``reseed_after_execution`` is False, the random seed is
      constantly ``seed`` across executions.
    - If ``seed`` is not None and ``reseed_after_execution`` is True, the random seed is
      different for each execution.

    .. note::
        Even if you provided a seed, you might still observe a non-deterministic row
        order. This is because tasks are executed in parallel and their completion
        order might vary. If you need to preserve the order of rows, set
        ``DataContext.get_current().execution_options.preserve_order``.

    Args:
        seed: An optional integer seed for the random operation. If None, the operation is
            non-deterministic. If provided, the operation is deterministic based on this
            seed and the ``reseed_after_execution`` setting.
        reseed_after_execution: If True, the random seed considers both ``seed`` and
            ``execution_idx``, resulting in different shuffling orders across executions.
            If False, the random seed is constantly ``seed``, resulting in the same
            shuffling order across executions. Only takes effect when ``seed`` is not None.
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
    ) -> tuple[int] | tuple[int, int] | None:
        """Return a tuple of seeds for random number generation.

        Args:
            data_context: A DataContext object for extracting the
                execution index.

        Returns:
            A tuple of seeds.
        """
        if self.seed is None:
            return None
        elif self.reseed_after_execution:
            return (data_context._execution_idx, self.seed)
        else:
            return (self.seed,)

    @classmethod
    def create_seed_config(
        cls,
        seed: int | RandomSeedConfig | None,
    ) -> RandomSeedConfig:
        """Create a ``RandomSeedConfig`` object from the ``seed`` argument in Ray Data public random APIs.

        This is a helper function that converts an integer seed into a ``RandomSeedConfig`` object. In this case,
        ``reseed_after_execution`` is set to False, which matches the standard behavior for a data pipeline
        (i.e., same seed across executions). To override this behavior, use a ``RandomSeedConfig`` object
        directly.

        Args:
            seed: This optional argument can be an integer or an existing ``RandomSeedConfig`` object.

        Returns:
            A ``RandomSeedConfig`` object.
        """
        if seed is None or isinstance(seed, int):
            return RandomSeedConfig(
                seed=seed,
                reseed_after_execution=False,
            )
        elif isinstance(seed, RandomSeedConfig):
            return seed

        raise ValueError(f"Invalid seed type: {type(seed)}")


def get_timestamp_seed() -> int:
    return time.time_ns() % INT32_MAX


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
        A single integer random seed.
    """
    seed_tuple = seed_config.get_seed_tuple(data_context=data_context)

    if seed_tuple is None:
        # This is a legacy behavior for some random operations.
        return get_timestamp_seed() if seed_config.use_timestamp_as_default else None
    elif len(seed_tuple) == 1:
        return seed_tuple[0]

    # Modulo ensures the result is in valid NumPy seed range [0, 2**32 - 1].
    return hash(seed_tuple) % (2**32)
