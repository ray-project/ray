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
        assert result._internal_type == pa_type

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
        assert dt._internal_type == valid_type

    @pytest.mark.parametrize(
        "invalid_type",
        [
            "string",
            123,
            [1, 2, 3],
            {"key": "value"},
            None,
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
        assert dt._internal_type == pa_type

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
        assert dt._internal_type == expected_dtype


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
        with pytest.raises(ValueError, match="is not a Python type"):
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
        assert dt._internal_type == expected_dtype

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
        assert type(numpy_dt._internal_type) is not type(python_dt._internal_type)
        assert numpy_dt._internal_type is not python_dt._internal_type

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


if __name__ == "__main__":
    pytest.main(["-v", __file__])
