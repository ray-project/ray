import abc
from typing import Any, Optional, Dict, List, Tuple

from ray.rllib.utils.typing import TensorType

_INVALID_INPUT_DUP_DIM = "Duplicate dimension names in shape ({})"
_INVALID_INPUT_Unknown_DIM = "Unknown dimension name {} in shape ({})"
_INVALID_INPUT_ZERO_DIM = "Dimension {} in ({}) has size 0"
_INVALID_INPUT_NONINT_DIM = "Dimension {} in ({}) cannot be non-integer, got {}"
_INVALID_SHAPE = "Expected shape {} but found {}"
_INVALID_DTYPE = "Expected dtype {} but found {}"


class TensorSpecs(abc.ABC):
    """A base class that specifies the shape and dtype of a tensor."""

    def __init__(self, 
        shape: str, *, 
        dtype: Optional[Any] = None, 
        **shape_vals: Dict[str, int]
    ) -> None:
        self._expected_shape = self._parse_expected_shape(shape, shape_vals)
        self._dtype = dtype

    @property
    def shape(self):
        """Returns a `tuple` specifying the tensor shape."""
        return self._shape

    @property
    def dtype(self):
        """Returns a dtype specifying the tensor dtype."""
        return self._dtype

    def validate(self, tensor: TensorType) -> None:
        shape = self.get_shape(tensor)
        if len(shape) != len(self._expected_shape):
            raise ValueError(_INVALID_SHAPE.format(self._expected_shape, shape))
        
        for expected_d, actual_d in zip(self._expected_shape, shape):
            if isinstance(expected_d, int) and expected_d != actual_d:
                raise ValueError(_INVALID_SHAPE.format(self._expected_shape, shape))

        dtype = self.get_dtype(tensor)
        if self.dtype and dtype != self.dtype:
            raise ValueError(_INVALID_DTYPE.format(self.dtype, tensor.dtype))
    
    @abc.abstractmethod
    def get_shape(self, tensor: TensorType) -> Tuple[int]:
        """Returns the shape of a tensor."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_dtype(self, tensor: TensorType) -> Any:
        """Returns the data type of a tensor."""
        raise NotImplementedError

    @abc.abstractmethod
    def sample(self, fill_value: float = 0.0) -> TensorType:
        raise NotImplementedError

    def _parse_expected_shape(self, shape: str, shape_vals: Dict[str, int]) -> tuple:
        expected_shape = tuple()

        # check the validity of shape_vals and get a list of dimension names
        d_names = self._validate_shape_vals(shape, shape_vals)
        
        for d in d_names:
            d_value = shape_vals.get(d, None)
            if d_value is None:
                d_value = d
            expected_shape += (d if d_value is None else d_value, )

        return expected_shape


    def _validate_shape_vals(self, shape: str, shape_vals: Dict[str, int]) -> List[str]:
        d_names = shape.split(" ")
        d_names_set = set(d_names)
        if len(d_names_set) != len(d_names):
            raise ValueError(_INVALID_INPUT_DUP_DIM.format(",".join(d_names)))

        for d_name in shape_vals:
            if d_name not in d_names_set:
                raise ValueError(
                    _INVALID_INPUT_Unknown_DIM.format(d_name, ",".join(d_names))
                )
            
            d_value = shape_vals.get(d_name, None)
            if d_value is not None:
                if not isinstance(d_value, int):
                    raise ValueError(
                        _INVALID_INPUT_NONINT_DIM.format(d_name, shape, type(d_value))
                    )
                if d_value == 0:
                    raise ValueError(
                        _INVALID_INPUT_ZERO_DIM.format(d_name, ",".join(d_names))
                    )
        
        return d_names

    def __repr__(self) -> str:
        return f"TensorSpec(shape={tuple(self._expected_shape)}, dtype={self.dtype})"

    def __eq__(self, other: "TensorSpecs") -> bool:
        """Checks if the shape and dtype of two specs are equal."""
        if not isinstance(other, TensorSpecs):
            return False
        return self.shape == other.shape and self.dtype == other.dtype

    def __ne__(self, other: "TensorSpecs") -> bool:
        return not self == other