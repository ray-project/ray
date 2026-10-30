from typing import Any, List, Optional, Sequence, Tuple, Union

import gymnasium as gym
import numpy as np
from numpy.typing import DTypeLike

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class Simplex(gym.Space):
    """Represents a d - 1 dimensional Simplex in R^d.

    That is, all coordinates are in [0, 1] and sum to 1.
    The dimension d of the simplex is assumed to be shape[-1].

    Additionally one can specify the underlying distribution of
    the simplex as a Dirichlet distribution by providing concentration
    parameters. By default, sampling is uniform, i.e. concentration is
    all 1s.

    Example usage:
    self.action_space = spaces.Simplex(shape=(3, 4))
        --> 3 independent 4d Dirichlet with uniform concentration

    Args:
        shape: The shape of the space, as a tuple or list. The last dimension is d.
        concentration: The Dirichlet concentration parameters, an array of length
            d. Defaults to all 1s, for uniform sampling.
        dtype: The dtype of the space's samples.
    """

    def __init__(
        self,
        shape: Union[Tuple[int, ...], List[int]],
        concentration: Optional[np.ndarray] = None,
        dtype: DTypeLike = np.float32,
    ):
        assert type(shape) in [tuple, list]

        super().__init__(shape, dtype)
        self.dim = self.shape[-1]

        if concentration is not None:
            assert (
                concentration.shape[0] == shape[-1]
            ), f"{concentration.shape[0]} vs {shape[-1]}"
            self.concentration = concentration
        else:
            self.concentration = np.array([1] * self.dim)

    def sample(self):
        return np.random.dirichlet(self.concentration, size=self.shape[:-1]).astype(
            self.dtype
        )

    def contains(self, x: np.ndarray) -> bool:
        """Returns whether `x` is a valid member of this space.

        Args:
            x: The array to check.

        Returns:
            True if `x` has this space's shape and sums to 1 along the last axis.
        """
        return x.shape == self.shape and np.allclose(
            np.sum(x, axis=-1), np.ones_like(x[..., 0])
        )

    def to_jsonable(self, sample_n: Sequence[np.ndarray]) -> list:
        """Converts a batch of samples to a JSON-serializable list.

        Args:
            sample_n: The samples to convert.

        Returns:
            The samples as a nested list.
        """
        return np.array(sample_n).tolist()

    def from_jsonable(self, sample_n: Sequence[Any]) -> List[np.ndarray]:
        """Converts a JSON-serialized batch of samples back to numpy arrays.

        Args:
            sample_n: The serialized samples.

        Returns:
            A list with one numpy array per sample.
        """
        return [np.asarray(sample) for sample in sample_n]

    def __repr__(self):
        return "Simplex({}; {})".format(self.shape, self.concentration)

    def __eq__(self, other):
        return (
            np.allclose(self.concentration, other.concentration)
            and self.shape == other.shape
        )
