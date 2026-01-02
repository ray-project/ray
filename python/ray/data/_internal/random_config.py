import time
from dataclasses import dataclass
from typing import Optional, Union

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

    """  # noqa: E501

    seed: Optional[int] = None
    reseed_after_execution: bool = True

    def __post_init__(self):
        """Ensure that the seed is either None or an integer."""
        if self.seed is not None and not isinstance(self.seed, int):
            raise ValueError("Seed must be an integer or None.")

    def _get_seed_tuple(
        self,
        *,
        data_context: DataContext,
    ) -> Optional[tuple[int] | tuple[int, int]]:
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
    def _normalize_seed_argument(
        cls,
        seed: Optional[Union[int, "RandomSeedConfig"]],
        default_reseed_after_execution: bool,
    ) -> "RandomSeedConfig":
        """Return a RandomSeedConfig object based on the seed argument in
        Ray Data API.

        Args:
            seed: A seed argument accepted by the Ray Data API.
            default_reseed_after_execution: A default value of reseed_after_execution
                for an integer seed. If seed is None, this value is still passed to
                the RandomSeedConfig object but ignored.

        Returns:
            A RandomSeedConfig object.
        """
        if seed is None or isinstance(seed, int):
            return RandomSeedConfig(
                seed=seed, reseed_after_execution=default_reseed_after_execution
            )
        elif isinstance(seed, RandomSeedConfig):
            return seed

        raise ValueError(f"Invalid seed type: {type(seed)}")


def get_timestamp_seed() -> int:
    return time.time_ns() % INT32_MAX


def get_single_integer_random_seed(
    seed_config: RandomSeedConfig,
    data_context: DataContext,
    *,
    use_timestamp_as_default: bool,
) -> Optional[int]:
    """This function returns a single integer seed based on the ``RandomSeedConfig`` object. This
    is useful when the caller needs a single integer instead of a seed tuple. Optionally,
    when the base seed is None and ``use_timestamp_as_default`` is True, the timestamp is used
    to generate a seed. Otherwise, the seed tuple is hashed to generate a single integer seed.

    Args:
        seed_config: The ``RandomSeedConfig`` object.
        data_context: The ``DataContext`` object used to generate the seed tuple.
        use_timestamp_as_default: If True, use the timestamp as the default seed if the
            base seed is None. Otherwise, return None.

    Returns:
        A single integer random seed.
    """
    seed_tuple = seed_config._get_seed_tuple(data_context=data_context)

    if seed_tuple is None:
        return get_timestamp_seed() if use_timestamp_as_default else None
    elif len(seed_tuple) == 1:
        return seed_tuple[0]

    # Modulo ensures the result is in valid NumPy seed range [0, 2**32 - 1].
    return hash(seed_tuple) % (2**32)
