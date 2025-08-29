"""
PyArrow compatibility and utility module.

This module provides a centralized interface for PyArrow functionality by
reusing existing utilities from other Ray modules.
"""

from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa

from ray.air.util.tensor_extensions.arrow import (
    _infer_pyarrow_type,
)


class PyArrowCompat:
    """
    Centralized PyArrow compatibility class that reuses existing utilities.
    """

    @classmethod
    def type_definitions(cls) -> Dict[str, Tuple[callable, str]]:
        """Create factory method definitions for common PyArrow types."""

        return {
            "int8": (pa.int8, "an 8-bit signed integer"),
            "int16": (pa.int16, "a 16-bit signed integer"),
            "int32": (pa.int32, "a 32-bit signed integer"),
            "int64": (pa.int64, "a 64-bit signed integer"),
            "uint8": (pa.uint8, "an 8-bit unsigned integer"),
            "uint16": (pa.uint16, "a 16-bit unsigned integer"),
            "uint32": (pa.uint32, "a 32-bit unsigned integer"),
            "uint64": (pa.uint64, "a 64-bit unsigned integer"),
            "float32": (pa.float32, "a 32-bit floating point number"),
            "float64": (pa.float64, "a 64-bit floating point number"),
            "string": (pa.string, "a variable-length string"),
            "bool": (pa.bool_, "a boolean value"),
            "binary": (pa.binary, "variable-length binary data"),
        }

    @staticmethod
    def factory_methods(cls: type):
        """Metaprogramming: Class decorator to generate factory methods for PyArrow types using from_arrow.

        This decorator automatically creates class methods for common PyArrow data types.
        Each generated method is a convenient factory that calls cls.from_arrow(pa.type()).

        Generated methods include:
        - Signed integers: int8, int16, int32, int64
        - Unsigned integers: uint8, uint16, uint32, uint64
        - Floating point: float32, float64
        - Other types: string, bool, binary

        Examples of generated methods:
            @classmethod
            def int32(cls):
                '''Create a DataType representing a 32-bit signed integer.

                Returns:
                    DataType: A DataType with PyArrow int32 type
                '''
                return cls.from_arrow(pa.int32())

            @classmethod
            def string(cls):
                '''Create a DataType representing a variable-length string.

                Returns:
                    DataType: A DataType with PyArrow string type
                '''
                return cls.from_arrow(pa.string())

        Usage:
            Instead of DataType.from_arrow(pa.int32()), you can use DataType.int32()
        """

        type_definitions = PyArrowCompat.type_definitions()

        for method_name, (pa_func, description) in type_definitions.items():

            def create_method(name, func, desc):
                def factory_method(cls):
                    return cls.from_arrow(func())

                factory_method.__doc__ = f"""Create a DataType representing {desc}.

            Returns:
                DataType: A DataType with PyArrow {name} type
            """
                factory_method.__name__ = name
                factory_method.__qualname__ = f"{cls.__name__}.{name}"
                return classmethod(factory_method)

            setattr(cls, method_name, create_method(method_name, pa_func, description))

        return cls

    @classmethod
    def infer_type(cls, values: Union[List[Any], np.ndarray]) -> Optional[pa.DataType]:
        """
        Infer PyArrow DataType from values using existing utility.

        Args:
            values: List or array of values to infer type from

        Returns:
            Inferred PyArrow DataType or None if inference fails
        """
        return _infer_pyarrow_type(values)
