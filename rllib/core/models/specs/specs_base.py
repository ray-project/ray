import abc
from copy import deepcopy
import numpy as np
from typing import Any, Optional, Dict, List, Tuple, Union, Type
from ray.rllib.utils import try_import_jax, try_import_tf, try_import_torch
from ray.rllib.utils.deprecation import Deprecated
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


@Deprecated(
    help="The Spec checking APIs have been deprecated and cancelled without "
    "replacement.",
    error=False,
)
class Spec(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def validate(self, data: Any) -> None:
        pass


@Deprecated(
    help="The Spec checking APIs have been deprecated and cancelled without "
    "replacement.",
    error=False,
)
class TypeSpec(Spec):
    def __init__(self, dtype: Type) -> None:
        self.dtype = dtype

    def __repr__(self):
        return f"TypeSpec({str(self.dtype)})"

    def validate(self, data: Any) -> None:
        if not isinstance(data, self.dtype):
            raise ValueError(_INVALID_TYPE.format(self.dtype, type(data)))

    def __eq__(self, other: "TypeSpec") -> bool:
        if not isinstance(other, TypeSpec):
            return False
        return self.dtype == other.dtype

    def __ne__(self, other: "TypeSpec") -> bool:
        return not self == other


@Deprecated(
    help="The Spec checking APIs have been deprecated and cancelled without "
    "replacement.",
    error=False,
)
class TensorSpec(Spec):
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

    def _get_expected_type(self) -> Type:
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

    def get_shape(self, tensor: TensorType) -> Tuple[int]:
        if self._framework == "tf2":
            return tuple(
                int(i) if i is not None else None for i in tensor.shape.as_list()
            )
        return tuple(tensor.shape)

    def get_dtype(self, tensor: TensorType) -> Any:
        return tensor.dtype

    @property
    def dtype(self) -> Any:
        return self._dtype

    @property
    def shape(self) -> Tuple[Union[int, str]]:
        return self._expected_shape

    @property
    def type(self) -> Type:
        return self._type

    @property
    def full_shape(self) -> Tuple[int]:
        return self._full_shape

    def rdrop(self, n: int) -> "TensorSpec":
        assert isinstance(n, int) and n >= 0, "n must be a positive integer or zero"
        copy_ = deepcopy(self)
        copy_._expected_shape = copy_.shape[:-n]
        copy_._full_shape = self._get_full_shape()
        return copy_

    def append(self, spec: "TensorSpec") -> "TensorSpec":
        copy_ = deepcopy(self)
        copy_._expected_shape = (*copy_.shape, *spec.shape)
        copy_._full_shape = self._get_full_shape()
        return copy_

    def validate(self, tensor: TensorType) -> None:
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

    def fill(self, fill_value: Union[float, int] = 0) -> TensorType:
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
        sampled_shape = tuple()
        for d in self._expected_shape:
            if isinstance(d, int):
                sampled_shape += (d,)
            else:
                sampled_shape += (1,)
        return sampled_shape

    def _parse_expected_shape(self, shape: str, shape_vals: Dict[str, int]) -> tuple:
        d_names = shape.replace(" ", "").split(",")
        self._validate_shape_vals(d_names, shape_vals)

        expected_shape = tuple(shape_vals.get(d, d) for d in d_names)

        return expected_shape

    def _validate_shape_vals(
        self, d_names: List[str], shape_vals: Dict[str, int]
    ) -> None:
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
        if not isinstance(other, TensorSpec):
            return False
        return self.shape == other.shape and self.dtype == other.dtype

    def __ne__(self, other: "TensorSpec") -> bool:
        return not self == other
