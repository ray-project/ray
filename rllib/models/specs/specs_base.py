import abc
from typing import Any, Optional, Dict, List, Tuple, Union, Type

from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.typing import TensorType

_INVALID_INPUT_DUP_DIM = "Duplicate dimension names in shape ({})"
_INVALID_INPUT_UNKNOWN_DIM = "Unknown dimension name {} in shape ({})"
_INVALID_INPUT_POSITIVE = "Dimension {} in ({}) must be positive, got {}"
_INVALID_INPUT_INT_DIM = "Dimension {} in ({}) must be integer, got {}"
_INVALID_SHAPE = "Expected shape {} but found {}"
_INVALID_TYPE = "Expected tensor type {} but found {}"


@DeveloperAPI
class SpecsAbstract(abc.ABC):
    @DeveloperAPI
    @abc.abstractstaticmethod
    def validate(self, data: Any) -> None:
        """Validates the given data against this spec.

        Args:
            data: The input to validate.

        Raises:
            ValueError: If the data does not match this spec.
        """


@DeveloperAPI
class TensorSpecs(SpecsAbstract):
    """A base class that specifies the shape and dtype of a tensor.

    Args:
        shape: A string representing einops notation of the shape of the tensor.
            For example, "B, C" represents a tensor with two dimensions, the first
            of which has size B and the second of which has size C. shape should
            consist of unique dimension names. For example having "B B" is invalid.
        dtype: The dtype of the tensor. If None, the dtype is not checked during
            validation. Also during Sampling the dtype is set the default dtype of
            the backend.
        shape_vals: An optional dictionary mapping some dimension names to their
            values. For example, if shape is "B, C" and shape_vals is {"C": 3}, then
            the shape of the tensor is (B, 3). B is to be determined during
            run-time but C is fixed to 3.

    Examples:
        >>> spec = TensorSpec("b,h", h=128, dtype=tf.float32)
        >>> spec.shape  # ('b', 128)
        >>> spec.validate(torch.rand(32, 128, dtype=torch.float32))  # passes
        >>> spec.validate(torch.rand(32, 64, dtype=torch.float32))   # raises ValueError
        >>> spec.validate(torch.rand(32, 128, dtype=torch.float64))  # raises ValueError

    Public Methods:
        validate: Checks if the shape and dtype of the tensor matches the
            specification.
        fill: creates a tensor with the specified value that is an
            example of a tensor that matches the specification.

    Abstract Methods:
        get_type: Returns the type of the tensor, e.g. tf.Tensor or torch.Tensor.
        get_shape: Returns the shape of the tensor depending on the backend.
        get_dtype: Returns the dtype of the tensor depending on the backend.
        _full: Creates a tensor with the specified value that
            has values of fill_value, shape of shape, and dtype of self.dtype.
    """

    def __init__(
        self, shape: str, *, dtype: Optional[Any] = None, **shape_vals: Dict[str, int]
    ) -> None:
        self._expected_shape = self._parse_expected_shape(shape, shape_vals)
        self._full_shape = self._get_full_shape()
        self._dtype = dtype

    @property
    def shape(self) -> Tuple[Union[int, str]]:
        """Returns a `tuple` specifying the abstract tensor shape (int and str)."""
        return self._expected_shape

    @property
    def full_shape(self) -> Tuple[int]:
        """Returns a `tuple` specifying the concrete tensor shape (only ints)."""
        return self._full_shape

    @property
    def dtype(self) -> Any:
        """Returns a dtype specifying the tensor dtype."""
        return self._dtype

    @override(SpecsAbstract)
    def validate(self, tensor: TensorType) -> None:
        """Checks if the shape and dtype of the tensor matches the specification.

        Args:
            tensor: The tensor to be validated.

        Raises:
            ValueError: If the shape or dtype of the tensor does not match the
        """

        expected_type = self.get_type()
        if not isinstance(tensor, expected_type):
            raise ValueError(_INVALID_TYPE.format(expected_type, type(tensor).__name__))

        shape = self.get_shape(tensor)
        if len(shape) != len(self._expected_shape):
            raise ValueError(_INVALID_SHAPE.format(self._expected_shape, shape))

        for expected_d, actual_d in zip(self._expected_shape, shape):
            if isinstance(expected_d, int) and expected_d != actual_d:
                raise ValueError(_INVALID_SHAPE.format(self._expected_shape, shape))

        dtype = self.get_dtype(tensor)
        if self.dtype and dtype != self.dtype:
            raise ValueError(_INVALID_TYPE.format(self.dtype, tensor.dtype))

    @classmethod
    @abc.abstractmethod
    def get_type(cls) -> Union[Type, Tuple[Type]]:
        """Returns the type of a tensor e.g. torch.Tensor or tf.Tensor.

        Returns:
            The type of a tensor. If the backend supports multiple tensor types, then a
            tuple of types is returned.
        """

    @abc.abstractmethod
    def get_shape(self, tensor: TensorType) -> Tuple[int]:
        """Returns the shape of a tensor.

        Args:
            tensor: The tensor whose shape is to be returned.

        Returns:
            A `tuple` specifying the shape of the tensor.
        """

    @abc.abstractmethod
    def get_dtype(self, tensor: TensorType) -> Any:
        """Returns the data type of a tensor.

        Args:
            tensor: The tensor whose data type is to be returned.

        Returns:
            The data type of the tensor.
        """

    @DeveloperAPI
    def fill(self, fill_value: Union[float, int] = 0) -> TensorType:
        """Creates a tensor filled with `fill_value` that matches the specs.

        Args:
            fill_value: The value to fill the tensor with.

        Returns:
            A tensor with the specified value that matches the specs.
        """
        return self._full(self.full_shape, fill_value)

    @abc.abstractmethod
    def _full(self, shape: Tuple[int], fill_value: Union[float, int] = 0) -> TensorType:
        """Creates a tensor with the given shape filled with `fill_value`. The tensor
        dtype is inferred from `fill_value`. This is equivalent to np.full(shape, val).

        Args:
            shape: The shape of the tensor to be sampled.
            fill_value: The value to fill the tensor with.

        Returns:
            A tensor with the specified value that matches the specs.
        """

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
    ) -> List[str]:
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

    def __eq__(self, other: "TensorSpecs") -> bool:
        """Checks if the shape and dtype of two specs are equal."""
        if not isinstance(other, TensorSpecs):
            return False
        return self.shape == other.shape and self.dtype == other.dtype

    def __ne__(self, other: "TensorSpecs") -> bool:
        return not self == other
