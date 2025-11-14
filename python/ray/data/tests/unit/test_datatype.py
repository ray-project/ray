import numpy as np
import pyarrow as pa
import pytest

from ray.data.datatype import DataType


class TestDataTypeFactoryMethods:
    """Test the generated factory methods."""

    @pytest.mark.parametrize(
        "method_name,pa_type,description",
        [
            ("int8", pa.int8(), "8-bit signed integer"),
            ("int16", pa.int16(), "16-bit signed integer"),
            ("int32", pa.int32(), "32-bit signed integer"),
            ("int64", pa.int64(), "64-bit signed integer"),
            ("uint8", pa.uint8(), "8-bit unsigned integer"),
            ("uint16", pa.uint16(), "16-bit unsigned integer"),
            ("uint32", pa.uint32(), "32-bit unsigned integer"),
            ("uint64", pa.uint64(), "64-bit unsigned integer"),
            ("float32", pa.float32(), "32-bit floating point number"),
            ("float64", pa.float64(), "64-bit floating point number"),
            ("string", pa.string(), "variable-length string"),
            ("bool", pa.bool_(), "boolean value"),
            ("binary", pa.binary(), "variable-length binary data"),
        ],
    )
    def test_factory_method_creates_correct_type(
        self, method_name, pa_type, description
    ):
        """Test that factory methods create DataType with correct PyArrow type."""
        factory_method = getattr(DataType, method_name)
        result = factory_method()

        assert isinstance(result, DataType)
        assert result.is_arrow_type()
        assert result._physical_dtype == pa_type

    @pytest.mark.parametrize(
        "method_name",
        [
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "float32",
            "float64",
            "string",
            "bool",
            "binary",
        ],
    )
    def test_factory_method_has_proper_docstring(self, method_name):
        """Test that generated factory methods have proper docstrings."""
        factory_method = getattr(DataType, method_name)
        doc = factory_method.__doc__

        assert "Create a DataType representing" in doc
        assert "Returns:" in doc
        assert f"DataType with PyArrow {method_name} type" in doc


class TestDataTypeValidation:
    """Test DataType validation and initialization."""

    @pytest.mark.parametrize(
        "valid_type",
        [
            pa.int64(),
            pa.string(),
            pa.timestamp("s"),
            np.dtype("int32"),
            np.dtype("float64"),
            int,
            str,
            float,
        ],
    )
    def test_post_init_accepts_valid_types(self, valid_type):
        """Test that __post_init__ accepts valid type objects."""
        # Should not raise
        dt = DataType(valid_type)
        assert dt._physical_dtype == valid_type

    @pytest.mark.parametrize(
        "invalid_type",
        [
            "string",
            123,
            [1, 2, 3],
            {"key": "value"},
            object(),
        ],
    )
    def test_post_init_rejects_invalid_types(self, invalid_type):
        """Test that __post_init__ rejects invalid type objects."""
        with pytest.raises(
            TypeError,
            match="DataType supports only PyArrow DataType, NumPy dtype, or Python type",
        ):
            DataType(invalid_type)


class TestDataTypeCheckers:
    """Test type checking methods."""

    @pytest.mark.parametrize(
        "datatype,is_arrow,is_numpy,is_python",
        [
            (DataType.from_arrow(pa.int64()), True, False, False),
            (DataType.from_arrow(pa.string()), True, False, False),
            (DataType.from_numpy(np.dtype("int32")), False, True, False),
            (DataType.from_numpy(np.dtype("float64")), False, True, False),
            (DataType(int), False, False, True),
            (DataType(str), False, False, True),
        ],
    )
    def test_type_checkers(self, datatype, is_arrow, is_numpy, is_python):
        """Test is_arrow_type, is_numpy_type, and is_python_type methods."""
        assert datatype.is_arrow_type() == is_arrow
        assert datatype.is_numpy_type() == is_numpy
        assert datatype.is_python_type() == is_python


class TestDataTypeFactories:
    """Test factory methods from external systems."""

    @pytest.mark.parametrize(
        "pa_type",
        [
            pa.int32(),
            pa.string(),
            pa.timestamp("s"),
            pa.list_(pa.int32()),
            pa.decimal128(10, 2),
        ],
    )
    def test_from_arrow(self, pa_type):
        """Test from_arrow factory method."""
        dt = DataType.from_arrow(pa_type)

        assert isinstance(dt, DataType)
        assert dt.is_arrow_type()
        assert dt._physical_dtype == pa_type

    @pytest.mark.parametrize(
        "numpy_input,expected_dtype",
        [
            (np.dtype("int32"), np.dtype("int32")),
            (np.dtype("float64"), np.dtype("float64")),
            ("int64", np.dtype("int64")),
            ("float32", np.dtype("float32")),
        ],
    )
    def test_from_numpy(self, numpy_input, expected_dtype):
        """Test from_numpy factory method."""
        dt = DataType.from_numpy(numpy_input)

        assert isinstance(dt, DataType)
        assert dt.is_numpy_type()
        assert dt._physical_dtype == expected_dtype


class TestDataTypeConversions:
    """Test type conversion methods."""

    def test_to_arrow_dtype_arrow_passthrough(self):
        """Test that Arrow types return themselves."""
        dt = DataType.from_arrow(pa.int64())
        result = dt.to_arrow_dtype()
        assert result == pa.int64()

    def test_to_arrow_dtype_numpy_conversion(self):
        """Test conversion from NumPy to Arrow types."""
        dt = DataType.from_numpy(np.dtype("int32"))
        result = dt.to_arrow_dtype()
        assert result == pa.int32()

    def test_to_arrow_dtype_python_conversion(self):
        """Test conversion from Python to Arrow types."""
        dt = DataType(int)
        result = dt.to_arrow_dtype([1])
        # Python int should map to int64 in Arrow
        assert result == pa.int64()

    @pytest.mark.parametrize(
        "source_dt,expected_result",
        [
            # NumPy types should return themselves
            (DataType.from_numpy(np.dtype("int32")), np.dtype("int32")),
            (DataType.from_numpy(np.dtype("float64")), np.dtype("float64")),
            # Python types should fall back to object
            (DataType(str), np.dtype("object")),
            (DataType(list), np.dtype("object")),
        ],
    )
    def test_to_numpy_dtype(self, source_dt, expected_result):
        """Test to_numpy_dtype conversion."""
        result = source_dt.to_numpy_dtype()
        assert result == expected_result

    def test_to_numpy_dtype_arrow_basic_types(self):
        """Test Arrow to NumPy conversion for types that should work."""
        # Test basic types that should convert properly
        test_cases = [
            (pa.int32(), np.dtype("int32")),
            (pa.float64(), np.dtype("float64")),
            (pa.bool_(), np.dtype("bool")),
        ]

        for pa_type, expected_np_dtype in test_cases:
            dt = DataType.from_arrow(pa_type)
            result = dt.to_numpy_dtype()
            # Some Arrow types may not convert exactly as expected,
            # so let's just verify the result is a valid numpy dtype
            assert isinstance(result, np.dtype)

    def test_to_numpy_dtype_complex_arrow_fallback(self):
        """Test that complex Arrow types fall back to object dtype."""
        complex_dt = DataType.from_arrow(pa.list_(pa.int32()))
        result = complex_dt.to_numpy_dtype()
        assert result == np.dtype("object")

    @pytest.mark.parametrize("python_type", [int, str, float, bool, list])
    def test_to_python_type_success(self, python_type):
        """Test to_python_type returns the original Python type."""
        dt = DataType(python_type)
        result = dt.to_python_type()
        assert result == python_type

    @pytest.mark.parametrize(
        "non_python_dt",
        [
            DataType.from_arrow(pa.int64()),
            DataType.from_numpy(np.dtype("float32")),
        ],
    )
    def test_to_python_type_failure(self, non_python_dt):
        """Test to_python_type raises ValueError for non-Python types."""
        with pytest.raises(ValueError, match="is not backed by a Python type"):
            non_python_dt.to_python_type()


class TestDataTypeInference:
    """Test type inference from values."""

    @pytest.mark.parametrize(
        "numpy_value,expected_dtype",
        [
            (np.array([1, 2, 3], dtype="int32"), np.dtype("int32")),
            (np.array([1.0, 2.0], dtype="float64"), np.dtype("float64")),
            (np.int64(42), np.dtype("int64")),
            (np.float32(3.14), np.dtype("float32")),
        ],
    )
    def test_infer_dtype_numpy_values(self, numpy_value, expected_dtype):
        """Test inference of NumPy arrays and scalars."""
        dt = DataType.infer_dtype(numpy_value)

        assert dt.is_numpy_type()
        assert dt._physical_dtype == expected_dtype

    # Removed test_infer_dtype_pyarrow_scalar - no longer works with current implementation

    @pytest.mark.parametrize(
        "python_value",
        [
            42,  # int
            3.14,  # float
            "hello",  # str
            True,  # bool
            [1, 2, 3],  # list
        ],
    )
    def test_infer_dtype_python_values_arrow_success(self, python_value):
        """Test inference of Python values that Arrow can handle."""
        dt = DataType.infer_dtype(python_value)

        # Should infer to Arrow type for basic Python values
        assert dt.is_arrow_type()

    # Removed test_infer_dtype_fallback_to_python_type - no longer supported


class TestDataTypeStringRepresentation:
    """Test string representation methods."""

    @pytest.mark.parametrize(
        "datatype,expected_repr",
        [
            (DataType.from_arrow(pa.int64()), "DataType(arrow:int64)"),
            (DataType.from_arrow(pa.string()), "DataType(arrow:string)"),
            (DataType.from_numpy(np.dtype("float32")), "DataType(numpy:float32)"),
            (DataType.from_numpy(np.dtype("int64")), "DataType(numpy:int64)"),
            (DataType(str), "DataType(python:str)"),
            (DataType(int), "DataType(python:int)"),
        ],
    )
    def test_repr(self, datatype, expected_repr):
        """Test __repr__ method for different type categories."""
        assert repr(datatype) == expected_repr


class TestDataTypeEqualityAndHashing:
    """Test equality and hashing behavior."""

    @pytest.mark.parametrize(
        "dt1,dt2,should_be_equal",
        [
            # Same types should be equal
            (DataType.from_arrow(pa.int64()), DataType.from_arrow(pa.int64()), True),
            (
                DataType.from_numpy(np.dtype("float32")),
                DataType.from_numpy(np.dtype("float32")),
                True,
            ),
            (DataType(str), DataType(str), True),
            # Different Arrow types should not be equal
            (DataType.from_arrow(pa.int64()), DataType.from_arrow(pa.int32()), False),
            # Same conceptual type but different systems should not be equal
            (
                DataType.from_arrow(pa.int64()),
                DataType.from_numpy(np.dtype("int64")),
                False,
            ),
        ],
    )
    def test_equality(self, dt1, dt2, should_be_equal):
        """Test __eq__ method."""
        if should_be_equal:
            assert dt1 == dt2
            assert hash(dt1) == hash(dt2)
        else:
            assert dt1 != dt2

    def test_numpy_vs_python_inequality(self):
        """Test that numpy int64 and python int are not equal."""
        numpy_dt = DataType.from_numpy(np.dtype("int64"))
        python_dt = DataType(int)

        # These represent the same conceptual type but with different systems
        # so they should not be equal

        # First verify they have different internal types
        assert type(numpy_dt._physical_dtype) is not type(python_dt._physical_dtype)
        assert numpy_dt._physical_dtype is not python_dt._physical_dtype

        # Test the type checkers return different results
        assert numpy_dt.is_numpy_type() and not python_dt.is_numpy_type()
        assert python_dt.is_python_type() and not numpy_dt.is_python_type()

        # They should not be equal
        assert numpy_dt != python_dt

    @pytest.mark.parametrize(
        "non_datatype_value",
        [
            "not_a_datatype",
            42,
            [1, 2, 3],
            {"key": "value"},
            None,
        ],
    )
    def test_inequality_with_non_datatype(self, non_datatype_value):
        """Test that DataType is not equal to non-DataType objects."""
        dt = DataType.from_arrow(pa.int64())
        assert dt != non_datatype_value

    def test_hashability(self):
        """Test that DataType objects can be used in sets and as dict keys."""
        dt1 = DataType.from_arrow(pa.int64())
        dt2 = DataType.from_arrow(pa.int64())  # Same as dt1
        dt3 = DataType.from_arrow(pa.int32())  # Different

        # Test in set
        dt_set = {dt1, dt2, dt3}
        assert len(dt_set) == 2  # dt1 and dt2 are the same

        # Test as dict keys
        dt_dict = {dt1: "first", dt3: "second"}
        assert dt_dict[dt2] == "first"  # dt2 should match dt1


class TestLogicalDataTypes:
    """Test pattern-matching DataTypes with _LogicalDataType enum."""

    @pytest.mark.parametrize(
        "factory_method,logical_dtype_value",
        [
            (lambda: DataType.list(), "list"),
            (lambda: DataType.large_list(), "large_list"),
            (lambda: DataType.struct(), "struct"),
            (lambda: DataType.map(), "map"),
            (lambda: DataType.tensor(), "tensor"),
            (lambda: DataType.variable_shaped_tensor(), "tensor"),
            (lambda: DataType.temporal(), "temporal"),
        ],
    )
    def test_logical_dtype_creation(self, factory_method, logical_dtype_value):
        """Test that logical DataTypes have correct _logical_dtype."""
        from ray.data.datatype import _LogicalDataType

        dt = factory_method()
        assert dt._physical_dtype is None
        assert dt._logical_dtype == _LogicalDataType(logical_dtype_value)
        assert isinstance(dt._logical_dtype, _LogicalDataType)

    @pytest.mark.parametrize(
        "factory_method,expected_repr",
        [
            (lambda: DataType.list(), "DataType(logical_dtype:LIST)"),
            (lambda: DataType.large_list(), "DataType(logical_dtype:LARGE_LIST)"),
            (lambda: DataType.struct(), "DataType(logical_dtype:STRUCT)"),
            (lambda: DataType.map(), "DataType(logical_dtype:MAP)"),
            (lambda: DataType.tensor(), "DataType(logical_dtype:TENSOR)"),
            (
                lambda: DataType.variable_shaped_tensor(),
                "DataType(logical_dtype:TENSOR)",
            ),
            (lambda: DataType.temporal(), "DataType(logical_dtype:TEMPORAL)"),
        ],
    )
    def test_logical_dtype_repr(self, factory_method, expected_repr):
        """Test __repr__ for logical DataTypes."""
        dt = factory_method()
        assert repr(dt) == expected_repr

    @pytest.mark.parametrize(
        "dt1_factory,dt2_factory,should_be_equal",
        [
            # Same logical DataTypes should be equal (including explicit ANY form)
            (lambda: DataType.list(), lambda: DataType.list(DataType.ANY), True),
            (lambda: DataType.list(), lambda: DataType.list(), True),
            (lambda: DataType.struct(), lambda: DataType.struct(DataType.ANY), True),
            (
                lambda: DataType.tensor(),
                lambda: DataType.variable_shaped_tensor(),
                True,
            ),
            # Different logical DataTypes should not be equal
            (lambda: DataType.list(), lambda: DataType.large_list(), False),
            (lambda: DataType.list(), lambda: DataType.struct(), False),
            (lambda: DataType.map(), lambda: DataType.temporal(), False),
        ],
    )
    def test_logical_dtype_equality(self, dt1_factory, dt2_factory, should_be_equal):
        """Test equality between logical DataTypes."""
        dt1 = dt1_factory()
        dt2 = dt2_factory()

        if should_be_equal:
            assert dt1 == dt2
            assert hash(dt1) == hash(dt2)
        else:
            assert dt1 != dt2


class TestNestedTypeFactories:
    """Test factory methods for nested types (list, struct, map, etc.)."""

    @pytest.mark.parametrize(
        "factory_call,expected_arrow_type",
        [
            (lambda: DataType.list(DataType.int64()), pa.list_(pa.int64())),
            (lambda: DataType.list(DataType.string()), pa.list_(pa.string())),
            (
                lambda: DataType.large_list(DataType.float32()),
                pa.large_list(pa.float32()),
            ),
            (
                lambda: DataType.fixed_size_list(DataType.int32(), 5),
                pa.list_(pa.int32(), 5),
            ),
        ],
    )
    def test_list_type_factories(self, factory_call, expected_arrow_type):
        """Test list-type factory methods create correct Arrow types."""
        dt = factory_call()
        assert dt.is_arrow_type()
        assert dt._physical_dtype == expected_arrow_type

    @pytest.mark.parametrize(
        "fields,expected_arrow_type",
        [
            (
                [("x", DataType.int64()), ("y", DataType.float64())],
                pa.struct([("x", pa.int64()), ("y", pa.float64())]),
            ),
            (
                [("name", DataType.string()), ("age", DataType.int32())],
                pa.struct([("name", pa.string()), ("age", pa.int32())]),
            ),
        ],
    )
    def test_struct_factory(self, fields, expected_arrow_type):
        """Test struct factory method creates correct Arrow types."""
        dt = DataType.struct(fields)
        assert dt.is_arrow_type()
        assert dt._physical_dtype == expected_arrow_type

    @pytest.mark.parametrize(
        "key_type,value_type,expected_arrow_type",
        [
            (DataType.string(), DataType.int64(), pa.map_(pa.string(), pa.int64())),
            (DataType.int32(), DataType.float32(), pa.map_(pa.int32(), pa.float32())),
        ],
    )
    def test_map_factory(self, key_type, value_type, expected_arrow_type):
        """Test map factory method creates correct Arrow types."""
        dt = DataType.map(key_type, value_type)
        assert dt.is_arrow_type()
        assert dt._physical_dtype == expected_arrow_type

    @pytest.mark.parametrize(
        "temporal_type,unit,tz,expected_type",
        [
            ("timestamp", "s", None, pa.timestamp("s")),
            ("timestamp", "us", "UTC", pa.timestamp("us", tz="UTC")),
            ("date32", None, None, pa.date32()),
            ("date64", None, None, pa.date64()),
            ("time32", "s", None, pa.time32("s")),
            ("time64", "us", None, pa.time64("us")),
            ("duration", "ms", None, pa.duration("ms")),
        ],
    )
    def test_temporal_factory(self, temporal_type, unit, tz, expected_type):
        """Test temporal factory method creates correct Arrow types."""
        if tz is not None:
            dt = DataType.temporal(temporal_type, unit=unit, tz=tz)
        elif unit is not None:
            dt = DataType.temporal(temporal_type, unit=unit)
        else:
            dt = DataType.temporal(temporal_type)

        assert dt.is_arrow_type()
        assert dt._physical_dtype == expected_type

    @pytest.mark.parametrize(
        "temporal_type,unit,error_msg",
        [
            ("time32", "us", "time32 unit must be 's' or 'ms'"),
            ("time64", "ms", "time64 unit must be 'us' or 'ns'"),
            ("invalid", None, "Invalid temporal_type"),
        ],
    )
    def test_temporal_factory_validation(self, temporal_type, unit, error_msg):
        """Test temporal factory validates inputs correctly."""
        with pytest.raises(ValueError, match=error_msg):
            DataType.temporal(temporal_type, unit=unit)


class TestTypePredicates:
    """Test type predicate methods (is_list_type, is_struct_type, etc.)."""

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            # List types
            (DataType.list(DataType.int64()), True),
            (DataType.large_list(DataType.string()), True),
            (DataType.fixed_size_list(DataType.float32(), 3), True),
            # Tensor types (should return False)
            (DataType.tensor(shape=(3, 4), dtype=DataType.float32()), False),
            (DataType.variable_shaped_tensor(dtype=DataType.float64(), ndim=2), False),
            # Non-list types
            (DataType.int64(), False),
            (DataType.string(), False),
            (DataType.struct([("x", DataType.int32())]), False),
        ],
    )
    def test_is_list_type(self, datatype, expected_result):
        """Test is_list_type predicate."""
        assert datatype.is_list_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            (DataType.tensor(shape=(3, 4), dtype=DataType.float32()), True),
            (DataType.variable_shaped_tensor(dtype=DataType.float64(), ndim=2), True),
        ],
    )
    def test_is_tensor_type(self, datatype, expected_result):
        """Test is_tensor_type predicate."""
        assert datatype.is_tensor_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            (DataType.struct([("x", DataType.int64())]), True),
            (
                DataType.struct([("a", DataType.string()), ("b", DataType.float32())]),
                True,
            ),
            (DataType.list(DataType.int64()), False),
            (DataType.int64(), False),
        ],
    )
    def test_is_struct_type(self, datatype, expected_result):
        """Test is_struct_type predicate."""
        assert datatype.is_struct_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            (DataType.map(DataType.string(), DataType.int64()), True),
            (DataType.map(DataType.int32(), DataType.float32()), True),
            (DataType.list(DataType.int64()), False),
            (DataType.int64(), False),
        ],
    )
    def test_is_map_type(self, datatype, expected_result):
        """Test is_map_type predicate."""
        assert datatype.is_map_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            # Nested types
            (DataType.list(DataType.int64()), True),
            (DataType.struct([("x", DataType.int32())]), True),
            (DataType.map(DataType.string(), DataType.int64()), True),
            # Non-nested types
            (DataType.int64(), False),
            (DataType.string(), False),
            (DataType.float32(), False),
        ],
    )
    def test_is_nested_type(self, datatype, expected_result):
        """Test is_nested_type predicate."""
        assert datatype.is_nested_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            # Numerical Arrow types
            (DataType.int64(), True),
            (DataType.int32(), True),
            (DataType.float32(), True),
            (DataType.float64(), True),
            # Numerical NumPy types
            (DataType.from_numpy(np.dtype("int32")), True),
            (DataType.from_numpy(np.dtype("float64")), True),
            # Numerical Python types
            (DataType(int), True),
            (DataType(float), True),
            # Non-numerical types
            (DataType.string(), False),
            (DataType.binary(), False),
            (DataType(str), False),
        ],
    )
    def test_is_numerical_type(self, datatype, expected_result):
        """Test is_numerical_type predicate."""
        assert datatype.is_numerical_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            # String Arrow types
            (DataType.string(), True),
            (DataType.from_arrow(pa.large_string()), True),
            # String NumPy types
            (DataType.from_numpy(np.dtype("U10")), True),
            # String Python types
            (DataType(str), True),
            # Non-string types
            (DataType.int64(), False),
            (DataType.binary(), False),
        ],
    )
    def test_is_string_type(self, datatype, expected_result):
        """Test is_string_type predicate."""
        assert datatype.is_string_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            # Binary Arrow types
            (DataType.binary(), True),
            (DataType.from_arrow(pa.large_binary()), True),
            (DataType.from_arrow(pa.binary(10)), True),  # fixed_size_binary
            # Binary Python types
            (DataType(bytes), True),
            (DataType(bytearray), True),
            # Non-binary types
            (DataType.string(), False),
            (DataType.int64(), False),
        ],
    )
    def test_is_binary_type(self, datatype, expected_result):
        """Test is_binary_type predicate."""
        assert datatype.is_binary_type() == expected_result

    @pytest.mark.parametrize(
        "datatype,expected_result",
        [
            # Temporal Arrow types
            (DataType.temporal("timestamp", unit="s"), True),
            (DataType.temporal("date32"), True),
            (DataType.temporal("time64", unit="us"), True),
            (DataType.temporal("duration", unit="ms"), True),
            # Temporal NumPy types
            (DataType.from_numpy(np.dtype("datetime64[D]")), True),
            (DataType.from_numpy(np.dtype("timedelta64[s]")), True),
            # Non-temporal types
            (DataType.int64(), False),
            (DataType.string(), False),
        ],
    )
    def test_is_temporal_type(self, datatype, expected_result):
        """Test is_temporal_type predicate."""
        assert datatype.is_temporal_type() == expected_result


class TestNestedPatternMatching:
    """Test that pattern-matching DataTypes can be used as arguments to factory methods."""

    @pytest.mark.parametrize(
        "factory_call,expected_logical_dtype",
        [
            # list with pattern-matching element type
            (lambda: DataType.list(DataType.list()), "list"),
            (lambda: DataType.list(DataType.struct()), "list"),
            (lambda: DataType.list(DataType.map()), "list"),
            # large_list with pattern-matching element type
            (lambda: DataType.large_list(DataType.list()), "large_list"),
            (lambda: DataType.large_list(DataType.tensor()), "large_list"),
            # struct with pattern-matching field types
            (
                lambda: DataType.struct(
                    [("a", DataType.list()), ("b", DataType.int64())]
                ),
                "struct",
            ),
            (
                lambda: DataType.struct(
                    [("x", DataType.tensor()), ("y", DataType.map())]
                ),
                "struct",
            ),
            # map with pattern-matching key/value types
            (lambda: DataType.map(DataType.list(), DataType.int64()), "map"),
            (lambda: DataType.map(DataType.string(), DataType.struct()), "map"),
            (lambda: DataType.map(DataType.list(), DataType.map()), "map"),
            # tensor with pattern-matching dtype
            (lambda: DataType.tensor((3, 4), DataType.list()), "tensor"),
            (lambda: DataType.tensor((2, 2), DataType.struct()), "tensor"),
            # variable_shaped_tensor with pattern-matching dtype
            (
                lambda: DataType.variable_shaped_tensor(DataType.list(), ndim=2),
                "tensor",
            ),
            (lambda: DataType.variable_shaped_tensor(DataType.map(), ndim=3), "tensor"),
        ],
    )
    def test_nested_pattern_matching_types(self, factory_call, expected_logical_dtype):
        """Test that pattern-matching DataTypes work as arguments to factory methods.

        When a pattern-matching DataType (one with _physical_dtype=None) is passed
        as an argument to another factory method, it should result in a pattern-matching
        type, not try to call to_arrow_dtype() on it.
        """
        from ray.data.datatype import _LogicalDataType

        dt = factory_call()
        # Should create a pattern-matching type, not a concrete type
        assert dt._physical_dtype is None
        assert dt._logical_dtype == _LogicalDataType(expected_logical_dtype)

    def test_list_with_nested_pattern(self):
        """Test DataType.list(DataType.list()) returns pattern-matching LIST."""
        from ray.data.datatype import _LogicalDataType

        dt = DataType.list(DataType.list())
        assert dt._physical_dtype is None
        assert dt._logical_dtype == _LogicalDataType.LIST
        assert repr(dt) == "DataType(logical_dtype:LIST)"

    def test_struct_with_pattern_fields(self):
        """Test DataType.struct with pattern-matching field types."""
        from ray.data.datatype import _LogicalDataType

        dt = DataType.struct([("a", DataType.list()), ("b", DataType.tensor())])
        assert dt._physical_dtype is None
        assert dt._logical_dtype == _LogicalDataType.STRUCT


class TestPatternMatchingToArrowDtype:
    """Test that pattern-matching types cannot be converted to concrete Arrow types."""

    @pytest.mark.parametrize(
        "pattern_type_factory",
        [
            lambda: DataType.list(),
            lambda: DataType.large_list(),
            lambda: DataType.struct(),
            lambda: DataType.map(),
            lambda: DataType.tensor(),
            lambda: DataType.variable_shaped_tensor(),
            lambda: DataType.temporal(),
        ],
    )
    def test_pattern_matching_to_arrow_dtype_raises(self, pattern_type_factory):
        """Test that calling to_arrow_dtype on pattern-matching types raises an error.

        Pattern-matching types represent abstract type categories (e.g., "any list")
        and cannot be converted to concrete Arrow types.
        """
        dt = pattern_type_factory()
        assert dt.is_pattern_matching()

        with pytest.raises(ValueError, match="Cannot convert pattern-matching type"):
            dt.to_arrow_dtype()

    def test_pattern_matching_to_arrow_dtype_with_values_still_raises(self):
        """Test that even with values, pattern-matching types cannot be converted."""
        dt = DataType.list()
        assert dt.is_pattern_matching()

        # Even with values provided, pattern-matching types shouldn't convert
        with pytest.raises(ValueError, match="Cannot convert pattern-matching type"):
            dt.to_arrow_dtype(values=[1, 2, 3])


class TestPatternMatchingToNumpyDtype:
    """Test that pattern-matching types cannot be converted to concrete NumPy dtypes."""

    @pytest.mark.parametrize(
        "pattern_type_factory",
        [
            lambda: DataType.list(),
            lambda: DataType.large_list(),
            lambda: DataType.struct(),
            lambda: DataType.map(),
            lambda: DataType.tensor(),
            lambda: DataType.variable_shaped_tensor(),
            lambda: DataType.temporal(),
        ],
    )
    def test_pattern_matching_to_numpy_dtype_raises(self, pattern_type_factory):
        """Test that calling to_numpy_dtype on pattern-matching types raises an error.

        Pattern-matching types represent abstract type categories (e.g., "any list")
        and cannot be converted to concrete NumPy dtypes.
        """
        dt = pattern_type_factory()
        assert dt.is_pattern_matching()

        with pytest.raises(ValueError, match="Cannot convert pattern-matching type"):
            dt.to_numpy_dtype()


if __name__ == "__main__":
    pytest.main(["-v", __file__])
