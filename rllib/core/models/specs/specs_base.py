import abc
from copy import deepcopy
import numpy as np
from typing import Any, Optional, Dict, List, Tuple, Union, Type
from ray.rllib.utils import try_import_jax, try_import_tf, try_import_torch
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic

from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.typing import TensorType

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()
jax, _ = try_import_jax()

_INVALID_INPUT_DUP_DIM = "Duplicate dimension names in shape ({})"
_INVALID_INPUT_UNKNOWN_DIM = "Unknown dimension name {} in shape ({})"
_INVALID_INPUT_POSITIVE = "Dimension {} in ({}) must be positive, got {}"
_INVALID_INPUT_INT_DIM = "Dimension {} in ({}) must be integer, got {}"
_INVALID_SHAPE = "Expected shape {} but found {}"
_INVALID_TYPE = "Expected data type {} but found {}"


@DeveloperAPI
class Spec(abc.ABC):
    @DeveloperAPI
    @staticmethod
    @abc.abstractmethod
    def validate(self, data: Any) -> None:
        """Validates the given data against this spec.

        Args:
            data: The input to validate.

        Raises:
            ValueError: If the data does not match this spec.
        """


@DeveloperAPI
class TypeSpec(Spec):
    """A base class that checks the type of the input data.

    Args:
        dtype: The type of the object.

    Examples:
        >>> spec = TypeSpec(tf.Tensor)
        >>> spec.validate(tf.ones((2, 3))) # passes
        >>> spec.validate(torch.ones((2, 3))) # ValueError
    """

    def __init__(self, dtype: Type) -> None:
        self.dtype = dtype

    def __repr__(self):
        return f"TypeSpec({str(self.dtype)})"

    @override(Spec)
    def validate(self, data: Any) -> None:
        if not isinstance(data, self.dtype):
            raise ValueError(_INVALID_TYPE.format(self.dtype, type(data)))

    def __eq__(self, other: "TypeSpec") -> bool:
        if not isinstance(other, TypeSpec):
            return False
        return self.dtype == other.dtype

    def __ne__(self, other: "TypeSpec") -> bool:
        return not self == other


@DeveloperAPI
class TensorSpec(Spec):
    """A base class that specifies the shape and dtype of a tensor.

    Args:
        shape: A string representing einops notation of the shape of the tensor.
            For example, "B, C" represents a tensor with two dimensions, the first
            of which has size B and the second of which has size C. shape must
            consist of unique dimension names. For example having "B B" is invalid.
        dtype: The dtype of the tensor. If None, the dtype is not checked during
            validation. Also during Sampling the dtype is set the default dtype of
            the backend.
        framework: The framework of the tensor. If None, the framework is not
            checked during validation.
        shape_vals: An optional dictionary mapping some dimension names to their
            values. For example, if shape is "B, C" and shape_vals is {"C": 3}, then
            the shape of the tensor is (B, 3). B is to be determined during
            run-time but C is fixed to 3.

    Examples:
        >>> spec = TensorSpec("b, d", d=128, dtype=tf.float32)
        >>> spec.shape  # ('b', 128)
        >>> spec.validate(torch.rand(32, 128, dtype=torch.float32))  # passes
        >>> spec.validate(torch.rand(32, 64, dtype=torch.float32))   # raises ValueError
        >>> spec.validate(torch.rand(32, 128, dtype=torch.float64))  # raises ValueError

    Public Methods:
        validate: Checks if the shape and dtype of the tensor matches the
            specification.
        fill: creates a tensor with the specified value that is an
            example of a tensor that matches the specification. This can only be
            called if `framework` is specified.
    """

    def __init__(
        self,
        shape: str,
        *,
        dtype: Optional[Any] = None,
        framework: Optional[str] = None,
        **shape_vals: int,
    ) -> None:
        self._expected_shape = self._parse_expected_shape(shape, shape_vals)
        self._full_shape = self._get_full_shape()
        self._dtype = dtype
        self._framework = framework

        if framework not in ("tf2", "torch", "np", "jax", None):
            raise ValueError(f"Unknown framework {self._framework}")

        self._type = self._get_expected_type()

    @OverrideToImplementCustomLogic
    def _get_expected_type(self) -> Type:
        """Returns the expected type of the checked tensor."""
        if self._framework == "torch":
            return torch.Tensor
        elif self._framework == "tf2":
            return tf.Tensor
        elif self._framework == "np":
            return np.ndarray
        elif self._framework == "jax":
            jax, _ = try_import_jax()
            return jax.numpy.ndarray
        elif self._framework is None:
            # Don't restrict the type of the tensor if no framework is specified.
            return object

    @OverrideToImplementCustomLogic
    def get_shape(self, tensor: TensorType) -> Tuple[int]:
        """Returns the shape of a tensor.

        Args:
            tensor: The tensor whose shape is to be returned.
        Returns:
            A `tuple` specifying the shape of the tensor.
        """
        if self._framework == "tf2":
            # tf2 returns `Dimension` objects instead of `int` objects.
            return tuple(
                int(i) if i is not None else None for i in tensor.shape.as_list()
            )
        return tuple(tensor.shape)

    @OverrideToImplementCustomLogic
    def get_dtype(self, tensor: TensorType) -> Any:
        """Returns the expected data type of the checked tensor.

        Args:
            tensor: The tensor whose data type is to be returned.
        Returns:
            The data type of the tensor.
        """
        return tensor.dtype

    @property
    def dtype(self) -> Any:
        """Returns the expected data type of the checked tensor."""
        return self._dtype

    @property
    def shape(self) -> Tuple[Union[int, str]]:
        """Returns a `tuple` specifying the abstract tensor shape (int and str)."""
        return self._expected_shape

    @property
    def type(self) -> Type:
        """Returns the expected type of the checked tensor."""
        return self._type

    @property
    def full_shape(self) -> Tuple[int]:
        """Returns a `tuple` specifying the concrete tensor shape (only ints)."""
        return self._full_shape

    def rdrop(self, n: int) -> "TensorSpec":
        """Drops the last n dimensions.

        Returns a copy of this TensorSpec with the rightmost n dimensions removed.

        Args:
            n: A positive number of dimensions to remove from the right

        Returns:
            A copy of the tensor spec with the last n dims removed

        Raises:
            IndexError: If n is greater than the number of indices in self
            AssertionError: If n is negative or not an int
        """
        assert isinstance(n, int) and n >= 0, "n must be a positive integer or zero"
        copy_ = deepcopy(self)
        copy_._expected_shape = copy_.shape[:-n]
        copy_._full_shape = self._get_full_shape()
        return copy_

    def append(self, spec: "TensorSpec") -> "TensorSpec":
        """Appends the given TensorSpec to the self TensorSpec.

        Args:
            spec: The TensorSpec to append to the current TensorSpec

        Returns:
            A new tensor spec resulting from the concatenation of self and spec

        """
        copy_ = deepcopy(self)
        copy_._expected_shape = (*copy_.shape, *spec.shape)
        copy_._full_shape = self._get_full_shape()
        return copy_

    @override(Spec)
    def validate(self, tensor: TensorType) -> None:
        """Checks if the shape and dtype of the tensor matches the specification.

        Args:
            tensor: The tensor to be validated.

        Raises:
            ValueError: If the shape or dtype of the tensor does not match the
        """

        if not isinstance(tensor, self.type):
            raise ValueError(_INVALID_TYPE.format(self.type, type(tensor).__name__))

        shape = self.get_shape(tensor)
        if len(shape) != len(self._expected_shape):
            raise ValueError(_INVALID_SHAPE.format(self._expected_shape, shape))

        for expected_d, actual_d in zip(self._expected_shape, shape):
            if isinstance(expected_d, int) and expected_d != actual_d:
                raise ValueError(_INVALID_SHAPE.format(self._expected_shape, shape))

        dtype = tensor.dtype
        if self.dtype and dtype != self.dtype:
            raise ValueError(_INVALID_TYPE.format(self.dtype, tensor.dtype))

    @DeveloperAPI
    def fill(self, fill_value: Union[float, int] = 0) -> TensorType:
        """Creates a tensor filled with `fill_value` that matches the specs.

        Args:
            fill_value: The value to fill the tensor with.

        Returns:
            A tensor with the specified value that matches the specs.

        Raises:
            ValueError: If `framework` is not specified.
        """

        if self._framework == "torch":
            return torch.full(self.full_shape, fill_value, dtype=self.dtype)

        elif self._framework == "tf2":
            if self.dtype:
                return tf.ones(self.full_shape, dtype=self.dtype) * fill_value
            return tf.fill(self.full_shape, fill_value)

        elif self._framework == "np":
            return np.full(self.full_shape, fill_value, dtype=self.dtype)

        elif self._framework == "jax":
            return jax.numpy.full(self.full_shape, fill_value, dtype=self.dtype)

        elif self._framework is None:
            raise ValueError(
                "Cannot fill tensor without providing `framework` to TensorSpec. "
                "This TensorSpec was instantiated without `framework`."
            )

    def _get_full_shape(self) -> Tuple[int]:
        """Converts the expected shape to a shape by replacing the unknown dimension
        sizes with a value of 1."""
        sampled_shape = tuple()
        for d in self._expected_shape:
            if isinstance(d, int):
                sampled_shape += (d,)
            else:
                sampled_shape += (1,)
        return sampled_shape

    def _parse_expected_shape(self, shape: str, shape_vals: Dict[str, int]) -> tuple:
        """Converts the input shape to a tuple of integers and strings."""

        # check the validity of shape_vals and get a list of dimension names
        d_names = shape.replace(" ", "").split(",")
        self._validate_shape_vals(d_names, shape_vals)

        expected_shape = tuple(shape_vals.get(d, d) for d in d_names)

        return expected_shape

    def _validate_shape_vals(
        self, d_names: List[str], shape_vals: Dict[str, int]
    ) -> None:
        """Checks if the shape_vals is valid.

        Valid means that shape consist of unique dimension names and shape_vals only
        consists of keys that are in shape. Also shape_vals can only contain postive
        integers.
        """
        d_names_set = set(d_names)
        if len(d_names_set) != len(d_names):
            raise ValueError(_INVALID_INPUT_DUP_DIM.format(",".join(d_names)))

        for d_name in shape_vals:
            if d_name not in d_names_set:
                raise ValueError(
                    _INVALID_INPUT_UNKNOWN_DIM.format(d_name, ",".join(d_names))
                )

            d_value = shape_vals.get(d_name, None)
            if d_value is not None:
                if not isinstance(d_value, int):
                    raise ValueError(
                        _INVALID_INPUT_INT_DIM.format(
                            d_name, ",".join(d_names), type(d_value)
                        )
                    )
                if d_value <= 0:
                    raise ValueError(
                        _INVALID_INPUT_POSITIVE.format(
                            d_name, ",".join(d_names), d_value
                        )
                    )

    def __repr__(self) -> str:
        return f"TensorSpec(shape={tuple(self.shape)}, dtype={self.dtype})"

    def __eq__(self, other: "TensorSpec") -> bool:
        """Checks if the shape and dtype of two specs are equal."""
        if not isinstance(other, TensorSpec):
            return False
        return self.shape == other.shape and self.dtype == other.dtype

    def __ne__(self, other: "TensorSpec") -> bool:
        return not self == other
