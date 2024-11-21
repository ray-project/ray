#
# SPDX-FileCopyrightText: Copyright (c) 1993-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import Sequence, Union

import numpy as np

from onnxslim.third_party.onnx_graphsurgeon.logger import G_LOGGER
from onnxslim.third_party.onnx_graphsurgeon.util import misc


class Tensor:
    """Abstract base class for tensors in a graph."""

    DYNAMIC = -1

    def __init__(self):
        """**This class is abstract and cannot be constructed directly.**."""
        raise NotImplementedError("Tensor is an abstract class")

    def __setattr__(self, name, value):
        """Set an attribute, ensuring special handling for "inputs" and "outputs" properties."""
        if name in {"inputs", "outputs"}:
            try:
                attr = getattr(self, name)
                if value is attr:
                    # This can happen when using things like +=
                    # The __iadd__ is executed followed by an assignment
                    return

                attr.clear()
                attr.extend(value)
            except AttributeError:
                super().__setattr__(name, value)
        else:
            super().__setattr__(name, value)

    def is_empty(self):
        """
        Returns whether this tensor is considered empty in the graph.

        *Note: 'Empty' here refers to the name of the tensor, which is omitted for
        optional tensors, NOT the shape of the tensor*

        Returns:
            bool: Whether the tensor is empty, meaning that it is used for an omitted optional input or output.
        """
        return self.name == ""

    def to_constant(
        self,
        values: np.ndarray,
        data_location: int = None,
        export_dtype: Union[np.dtype, "onnx.TensorProto.DataType"] = None,
    ):
        """
        Modifies this tensor in-place to convert it to a Constant. This means that all consumers/producers of the tensor
        will see the update.

        Args:
            values (np.ndarray): The values in this tensor

            data_location (int):
                    An enum value indicating the location where the tensor data is stored.
                    Generally, this will come from onnx.TensorProto.DataLocation.

            dtype (Union[numpy.dtype, onnx.TensorProto.DataType]): The data type of the tensor.

        Returns:
            self
        """
        self.__class__ = Constant
        self._values = values
        self.data_location = data_location
        self.export_dtype = export_dtype

        return self

    def to_variable(
        self, dtype: Union[np.dtype, "onnx.TensorProto.DataType"] = None, shape: Sequence[Union[int, str]] = None
    ):
        """
        Modifies this tensor in-place to convert it to a Variable. This means that all consumers/producers of the tensor
        will see the update.

        Args:
            dtype (Union[numpy.dtype, onnx.TensorProto.DataType]): The data type of the tensor.
            shape (Sequence[int]): The shape of the tensor.

        Returns:
            self
        """
        if shape is None:
            shape = []
        variable_dtype = dtype if dtype is not None else self.export_dtype

        self.__class__ = Variable
        self.shape = shape
        self.dtype = variable_dtype

        return self

    def i(self, tensor_idx=0, producer_idx=0):
        """
        Convenience function to get an input tensor of one of this tensor's input nodes. Note that the parameters are
        swapped compared to the o() function; this is because tensors are likely to have only a single producer.

        For example:
        ::

            assert tensor.i() == tensor.inputs[0].inputs[0]
            assert tensor.i(1, 2) == tensor.inputs[2].inputs[1]

        Args:
            tensor_idx (int): The index of the input tensor of the input node. Defaults to 0.
            producer_idx (int): The index of the producer node of the input tensor, if the tensor has multiple producers. Defaults to 0.

        Returns:
            Tensor: The specified producer (input) tensor.
        """
        return self.inputs[producer_idx].inputs[tensor_idx]

    def o(self, consumer_idx=0, tensor_idx=0):
        """
        Convenience function to get an output tensor of one of this tensor's output nodes.

        For example:
        ::

            assert tensor.o() == tensor.outputs[0].outputs[0]
            assert tensor.o(2, 1) == tensor.outputs[2].outputs[1]

        Args:
            consumer_idx (int): The index of the consumer of the input tensor. Defaults to 0.
            tensor_idx (int): The index of the output tensor of the node, if the node has multiple outputs. Defaults to 0.

        Returns:
            Tensor: The specified consumer (output) tensor
        """
        return self.outputs[consumer_idx].outputs[tensor_idx]

    def __str__(self):
        """Returns a string representation of the object including its type, name, shape, and data type."""
        return f"{type(self).__name__} ({self.name}): (shape={self.shape}, dtype={self.dtype})"

    def __repr__(self):  # Hack to make logging output pretty.
        """Returns a string representation of the object for logging output."""
        return self.__str__()

    def __eq__(self, other):
        """
        Perform a check to see if two tensors are equal.

        Tensors are considered equal if they share the same name. A Graph must not include Tensors with duplicate names.
        """
        return self.name == other.name

    @property
    def is_input(self):
        """Indicates whether this tensor is an input tensor in the graph."""
        return self._is_input if hasattr(self, "_is_input") else False

    @is_input.setter
    def is_input(self, is_input: bool = False):
        """Indicates whether this tensor is an input tensor in the graph."""
        self._is_input = is_input

    @property
    def is_output(self):
        """Indicates if tensor is marked as an output within the computational graph."""
        return self._is_output if hasattr(self, "_is_output") else False

    @is_output.setter
    def is_output(self, is_output: bool = False):
        """Indicates if the tensor is used as an output in the graph."""
        self._is_output = is_output


class Variable(Tensor):
    @staticmethod
    def empty():
        """Create and return an empty Variable tensor with an empty name."""
        return Variable(name="")

    def __init__(
        self,
        name: str,
        dtype: Union[np.dtype, "onnx.TensorProto.DataType"] = None,
        shape: Sequence[Union[int, str]] = None,
        type: str = "tensor_type",
    ):
        """
        Represents a Tensor whose value is not known until inference-time.

        Args:
            name (str): The name of the tensor.
            dtype (Union[numpy.dtype, onnx.TensorProto.DataType]): The data type of the tensor.
            shape (Sequence[Union[int, str]]): The shape of the tensor. This may contain strings if the model uses dimension parameters.
            type (str): The type of the tensor.
        """
        self.name = name
        self.inputs = misc.SynchronizedList(self, field_name="outputs", initial=[])
        self.outputs = misc.SynchronizedList(self, field_name="inputs", initial=[])
        self.dtype = dtype
        self.shape = misc.default_value(shape, None)
        self.type = type

    def to_constant(
        self,
        values: np.ndarray,
        export_dtype: Union[np.dtype, "onnx.TensorProto.DataType"] = None,
    ):
        """Converts the Variable to a Constant with given values and optional export data type."""
        del self.dtype
        del self.shape

        return super().to_constant(values, export_dtype=export_dtype)

    def copy(self):
        """
        Makes a shallow copy of this tensor, omitting input and output information.

        Note: Generally, you should only ever make a copy of a Graph.
        """
        return Variable(self.name, self.dtype, self.shape)

    def __eq__(self, other):
        """Perform a check to see if two variables are equal."""
        if not isinstance(other, Variable):
            return False

        name_match = self.name == other.name
        inputs_match = len(self.inputs) == len(other.inputs) and all(
            inp.name == other_inp.name for inp, other_inp in zip(self.inputs, other.inputs)
        )
        outputs_match = len(self.outputs) == len(other.outputs) and all(
            out.name == other_out.name for out, other_out in zip(self.outputs, other.outputs)
        )

        dtype_match = self.dtype == other.dtype
        shape_match = self.shape == other.shape
        type_match = self.type == other.type

        return name_match and inputs_match and outputs_match and dtype_match and shape_match and type_match


class LazyValues:
    """A special object that represents constant tensor values that should be lazily loaded."""

    def __init__(self, tensor):
        """
        Args:
            tensor (onnx.TensorProto, onnx.SparseTensorProto): The ONNX tensor that this instance should lazily load.
        """
        from onnxslim.third_party.onnx_graphsurgeon.importers.onnx_importer import (
            get_itemsize,
            get_onnx_tensor_dtype,
            get_onnx_tensor_shape,
        )

        self.tensor = tensor
        self.shape = get_onnx_tensor_shape(self.tensor)
        self.dtype = get_onnx_tensor_dtype(self.tensor)
        self.nbytes = misc.volume(self.shape) * get_itemsize(self.dtype)

    def load(self):
        """
        Load a numpy array from the underlying tensor values.

        Returns:
            np.array: A numpy array containing the values of the tensor.
        """
        import onnx
        import onnx.numpy_helper

        from onnxslim.third_party.onnx_graphsurgeon.importers.onnx_importer import (
            get_dtype_name,
            get_numpy_type,
        )

        if get_numpy_type(self.dtype) is None:
            G_LOGGER.warning(
                f"Datatype: {get_dtype_name(self.dtype)} could not be converted to a NumPy type.\n"
                f"Accessing the values of this constant tensor ({self.tensor.name}) will cause them to be casted to a supported data type. "
                f"This means that the weights will have a different type than the original model when they are exported again!\n"
                f"If this is not what you intended, please avoid accessing the values of this constant tensor."
            )

        return np.array(onnx.numpy_helper.to_array(self.tensor))

    def __str__(self):
        """Returns a formatted string representation of the LazyValues object indicating its shape and dtype."""
        return f"LazyValues (shape={self.shape}, dtype={self.dtype})"

    def __repr__(self):  # Hack to make logging output pretty.
        """Returns an unambiguous string representation of the LazyValues object for logging purposes."""
        return self.__str__()

    def __eq__(self, other):
        """Perform a check to see if two variables are equal."""
        if not isinstance(other, LazyValues):
            return False

        for field in self.tensor.DESCRIPTOR.fields:
            if field.name == "name":
                continue
            if getattr(self.tensor, field.name) != getattr(other.tensor, field.name):
                return False

        return True


class SparseValues(LazyValues):
    """A special object that represents constant tensor values that is sparse."""

    def load(self):
        """
        Load a numpy array from the sparse structure.

        Returns:
            np.array: A numpy array containing the values of the tensor.
        """
        import onnx
        import onnx.numpy_helper

        supported_index_type = [onnx.TensorProto.INT64]
        if self.tensor.indices.data_type not in supported_index_type:
            G_LOGGER.critical(
                f"Unsupported index data type {self.tensor.indices.data_type} in {self.tensor.values.name}"
            )

        if self.tensor.values.data_type == onnx.TensorProto.FLOAT16:
            values_data = np.asarray(self.tensor.values.int32_data, dtype=np.uint16).view(np.float16)
        else:
            field_name = onnx.helper.tensor_dtype_to_field(self.tensor.values.data_type)
            values = getattr(self.tensor.values, field_name)
            dtype = onnx.helper.tensor_dtype_to_np_dtype(self.tensor.values.data_type)
            values_data = np.asarray(values, dtype)
        indices_data = self.tensor.indices.int64_data

        if len(self.tensor.indices.dims) == 1:
            values = np.zeros(np.prod(self.tensor.dims))
            # [NNZ] layout, in which case the i-th value must be the linearized-index of the i-th value.
            values[indices_data] = values_data
            values = values.reshape(self.tensor.dims)
        elif len(self.tensor.indices.dims) == 2:
            # [NNZ, rank] with the [i,j]-th value corresponding to the j-th index of the i-th value
            values = np.zeros(self.tensor.dims)
            indices_data = np.asarray(indices_data).reshape(self.tensor.indices.dims)

            for value_data, index_data in zip(values_data, indices_data):
                values[tuple(index_data)] = value_data
        else:
            G_LOGGER.critical(f"Unsupported index data dims {self.tensor.indices.dims} in {self.tensor.values.name}")

        return values

    def __str__(self):
        """Return a string representation of the SparseValues object with its shape and data type."""
        return f"SparseValues (shape={self.shape}, dtype={self.dtype})"


class Constant(Tensor):
    def __init__(
        self,
        name: str,
        values: Union[np.ndarray, LazyValues],
        data_location: int = None,
        export_dtype: Union[np.dtype, "onnx.TensorProto.DataType"] = None,
    ):
        """
        Represents a Tensor whose value is known.

        Args:
            name (str): The name of the tensor.
            values (numpy.ndarray): The values in this tensor, in the form of a NumPy array.

            data_location (int):
                    An enum value indicating the location where the tensor data is stored.
                    Generally, this will come from onnx.TensorProto.DataLocation.


            export_dtype (Union[np.dtype, onnx.TensorProto.DataType]):
                    The data type of the tensor when exported to onnx. If not specified, then
                    the data type of values will be used.
        """
        self.name = name
        self.inputs = misc.SynchronizedList(self, field_name="outputs", initial=[])
        self.outputs = misc.SynchronizedList(self, field_name="inputs", initial=[])
        if (
            not isinstance(values, np.ndarray)
            and not isinstance(values, LazyValues)
            and not isinstance(values, SparseValues)
        ):
            G_LOGGER.critical(
                "Provided `values` argument is not a NumPy array, a LazyValues instance or a"
                "SparseValues instance. Please provide a NumPy array or LazyValues instance "
                f"to construct a Constant. Note: Provided `values` parameter was: {values}"
            )
        self._values = values
        self.data_location = data_location
        self._export_dtype = export_dtype

    def to_variable(self, dtype: np.dtype = None, shape: Sequence[Union[int, str]] = None):
        """Convert instance values to an appropriate variable with specified dtype and shape."""
        if shape is None:
            shape = []
        del self._export_dtype
        del self._values

        if dtype is not None:
            return super().to_variable(dtype, shape)

        var_dtype = self.export_dtype

        return super().to_variable(var_dtype, shape)

    def copy(self):
        """
        Makes a shallow copy of this tensor, omitting input and output information.

        Note: Generally, you should only ever make a copy of a Graph.
        """
        return Constant(self.name, self._values, export_dtype=self.export_dtype)

    @property
    def values(self):
        """Return the values of the tensor, loading them if they are accessed for the first time."""
        if isinstance(self._values, LazyValues):
            self._values = self._values.load()
        return self._values

    @values.setter
    def values(self, values: Union[np.ndarray, LazyValues]):
        """Return the values of the tensor, loading them if accessed for the first time."""
        self._values = values

    @property
    def shape(self):
        """Return the shape of the tensor values."""
        return self._values.shape

    @property
    def dtype(self):
        """Return the data type (dtype) of the tensor values."""
        return self._values.dtype

    @property
    def export_dtype(self):
        """Return the export data type (export_dtype) of the tensor values if specified, otherwise None."""
        return self._export_dtype if self._export_dtype is not None else self.dtype

    @export_dtype.setter
    def export_dtype(self, export_dtype):
        """Return the export data type of tensor values if specified, otherwise return the default data type."""
        self._export_dtype = export_dtype

    def __repr__(self):  # Hack to make logging output pretty.
        """Return a string representation of the object, including its values, for improved logging readability."""
        ret = self.__str__()
        ret += f"\n{self._values}"
        return ret

    def __eq__(self, other):
        """Perform a check to see if two constants are equal."""
        if not isinstance(other, Constant):
            return False

        if self._values.shape != other._values.shape:
            return False

        if self._values.dtype != other._values.dtype:
            return False

        return (
            self._values == other._values
            if isinstance(self._values, LazyValues) and isinstance(other._values, LazyValues)
            else np.array_equal(self.values, other.values)
        )
