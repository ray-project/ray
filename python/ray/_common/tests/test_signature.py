"""Tests for Ray signature utility functions.

This module contains pytest-based tests for signature-related functions in
ray._common.signature. These functions are used for extracting, validating,
and flattening function signatures for serialization.
"""

import inspect
import pytest
import sys
from typing import Any, Optional
from unittest.mock import Mock, patch

from ray._common.signature import (
    get_signature,
    extract_signature,
    validate_args,
    flatten_args,
    recover_args,
    DUMMY_TYPE,
)


class TestGetSignature:
    """Tests for the get_signature utility function."""

    def test_regular_function(self):
        """Test getting signature from a regular Python function."""

        def test_func(a, b=10, *args, **kwargs):
            return a + b

        sig = get_signature(test_func)
        assert sig is not None
        assert len(sig.parameters) == 4
        assert "a" in sig.parameters
        assert "b" in sig.parameters
        assert sig.parameters["b"].default == 10

    def test_function_with_annotations(self):
        """Test getting signature from a function with type annotations."""

        def test_func(a: int, b: str = "default") -> str:
            return f"{a}{b}"

        sig = get_signature(test_func)
        assert sig is not None
        assert len(sig.parameters) == 2
        assert sig.parameters["a"].annotation is int
        assert sig.parameters["b"].annotation is str
        assert sig.parameters["b"].default == "default"

    def test_function_no_parameters(self):
        """Test getting signature from a function with no parameters."""

        def test_func():
            return "hello"

        sig = get_signature(test_func)
        assert sig is not None
        assert len(sig.parameters) == 0

    def test_lambda_function(self):
        """Test getting signature from a lambda function."""
        sig = get_signature(lambda x, y=5: x + y)
        assert sig is not None
        assert len(sig.parameters) == 2  # x, y
        assert sig.parameters["y"].default == 5

    @patch("ray._common.signature.is_cython")
    def test_cython_function_with_attributes(self, mock_is_cython):
        """Test getting signature from a Cython function with required attributes."""
        mock_is_cython.return_value = True

        def original_func(x=10):
            return x

        mock_func = Mock()
        mock_func.__code__ = original_func.__code__
        mock_func.__annotations__ = original_func.__annotations__
        mock_func.__defaults__ = original_func.__defaults__
        mock_func.__kwdefaults__ = original_func.__kwdefaults__

        sig = get_signature(mock_func)
        assert sig is not None
        assert len(sig.parameters) == 1
        assert "x" in sig.parameters

    @patch("ray._common.signature.is_cython")
    def test_cython_function_missing_attributes(self, mock_is_cython):
        """Test error handling for Cython function missing required attributes."""
        mock_is_cython.return_value = True

        # Create a mock Cython function missing required attributes
        mock_func = Mock()
        del mock_func.__code__  # Remove required attribute

        with pytest.raises(TypeError, match="is not a Python function we can process"):
            get_signature(mock_func)

    def test_method_signature(self):
        """Test getting signature from a class method."""

        class TestClass:
            def test_method(self, a, b=20):
                return a + b

        sig = get_signature(TestClass.test_method)
        assert sig is not None
        assert len(sig.parameters) == 3  # self, a, b
        assert "self" in sig.parameters
        assert "a" in sig.parameters
        assert "b" in sig.parameters
        assert sig.parameters["b"].default == 20


class TestExtractSignature:
    """Tests for the extract_signature utility function."""

    def test_function_without_ignore_first(self):
        """Test extracting signature from function without ignoring first parameter."""

        def test_func(a, b=10, c=None):
            return a + b

        params = extract_signature(test_func, ignore_first=False)
        assert len(params) == 3
        assert params[0].name == "a"
        assert params[1].name == "b"
        assert params[1].default == 10
        assert params[2].name == "c"
        assert params[2].default is None

    def test_method_with_ignore_first(self):
        """Test extracting signature from method ignoring 'self' parameter."""

        class TestClass:
            def test_method(self, a, b=20):
                return a + b

        params = extract_signature(TestClass.test_method, ignore_first=True)
        assert len(params) == 2
        assert params[0].name == "a"
        assert params[1].name == "b"
        assert params[1].default == 20

    def test_function_with_ignore_first(self):
        """Test extracting signature from regular function with ignore_first=True."""

        def test_func(x, y, z=30):
            return x + y + z

        params = extract_signature(test_func, ignore_first=True)
        assert len(params) == 2
        assert params[0].name == "y"
        assert params[1].name == "z"
        assert params[1].default == 30

    def test_empty_parameters_with_ignore_first(self):
        """Test error handling when method has no parameters but ignore_first=True."""

        def test_func():
            return "hello"

        with pytest.raises(ValueError, match="Methods must take a 'self' argument"):
            extract_signature(test_func, ignore_first=True)

    def test_single_parameter_with_ignore_first(self):
        """Test extracting signature from method with only 'self' parameter."""

        class TestClass:
            def test_method(self):
                return "hello"

        params = extract_signature(TestClass.test_method, ignore_first=True)
        assert len(params) == 0

    def test_varargs_and_kwargs(self):
        """Test extracting signature with *args and **kwargs."""

        def test_func(a, b=10, *args, **kwargs):
            return a + b

        params = extract_signature(test_func, ignore_first=False)
        assert len(params) == 4
        assert params[0].name == "a"
        assert params[1].name == "b"
        assert params[2].name == "args"
        assert params[2].kind == inspect.Parameter.VAR_POSITIONAL
        assert params[3].name == "kwargs"
        assert params[3].kind == inspect.Parameter.VAR_KEYWORD


class TestValidateArgs:
    """Tests for the validate_args utility function."""

    def test_valid_positional_args(self):
        """Test validation with valid positional arguments."""

        def test_func(a, b, c=30):
            return a + b + c

        params = extract_signature(test_func)
        # Should not raise an exception
        validate_args(params, (1, 2), {})
        validate_args(params, (1, 2, 3), {})

    def test_valid_keyword_args(self):
        """Test validation with valid keyword arguments."""

        def test_func(a, b=20, c=30):
            return a + b + c

        params = extract_signature(test_func)
        # Should not raise an exception
        validate_args(params, (1,), {"b": 2})
        validate_args(params, (1,), {"b": 2, "c": 3})
        validate_args(params, (), {"a": 1, "b": 2, "c": 3})

    def test_valid_mixed_args(self):
        """Test validation with mixed positional and keyword arguments."""

        def test_func(a, b, c=30):
            return a + b + c

        params = extract_signature(test_func)
        # Should not raise an exception
        validate_args(params, (1,), {"b": 2})
        validate_args(params, (1, 2), {"c": 3})

    def test_too_many_positional_args(self):
        """Test error handling for too many positional arguments."""

        def test_func(a, b):
            return a + b

        params = extract_signature(test_func)
        with pytest.raises(TypeError):
            validate_args(params, (1, 2, 3), {})

    def test_missing_required_args(self):
        """Test error handling for missing required arguments."""

        def test_func(a, b, c=30):
            return a + b + c

        params = extract_signature(test_func)
        with pytest.raises(TypeError):
            validate_args(params, (1,), {})  # Missing 'b'

    def test_unexpected_keyword_args(self):
        """Test error handling for unexpected keyword arguments."""

        def test_func(a, b):
            return a + b

        params = extract_signature(test_func)
        with pytest.raises(TypeError):
            validate_args(params, (1, 2), {"c": 3})

    def test_duplicate_args(self):
        """Test error handling for duplicate arguments (positional and keyword)."""

        def test_func(a, b, c=30):
            return a + b + c

        params = extract_signature(test_func)
        with pytest.raises(TypeError):
            validate_args(params, (1, 2), {"b": 3})  # 'b' specified twice

    def test_varargs_validation(self):
        """Test validation with *args and **kwargs."""

        def test_func(a, b=20, *args, **kwargs):
            return a + b

        params = extract_signature(test_func)
        # Should not raise an exception
        validate_args(params, (1, 2, 3, 4), {"extra": 5})
        validate_args(params, (1,), {"b": 2, "extra": 3})


class TestFlattenArgs:
    """Tests for the flatten_args utility function."""

    def test_only_positional_args(self):
        """Test flattening with only positional arguments."""

        def test_func(a, b, c):
            return a + b + c

        params = extract_signature(test_func)
        flattened = flatten_args(params, (1, 2, 3), {})

        expected = [DUMMY_TYPE, 1, DUMMY_TYPE, 2, DUMMY_TYPE, 3]
        assert flattened == expected

    def test_only_keyword_args(self):
        """Test flattening with only keyword arguments."""

        def test_func(a=1, b=2, c=3):
            return a + b + c

        params = extract_signature(test_func)
        flattened = flatten_args(params, (), {"a": 10, "b": 20, "c": 30})

        expected = ["a", 10, "b", 20, "c", 30]
        assert flattened == expected

    def test_mixed_args(self):
        """Test flattening with mixed positional and keyword arguments."""

        def test_func(a, b, c=30):
            return a + b + c

        params = extract_signature(test_func)
        flattened = flatten_args(params, (1, 2), {"c": 3})

        expected = [DUMMY_TYPE, 1, DUMMY_TYPE, 2, "c", 3]
        assert flattened == expected

    def test_empty_args(self):
        """Test flattening with no arguments."""

        def test_func():
            return "hello"

        params = extract_signature(test_func)
        flattened = flatten_args(params, (), {})

        assert flattened == []

    def test_complex_types(self):
        """Test flattening with complex argument types."""

        def test_func(a, b, c=None):
            return a + b

        params = extract_signature(test_func)
        complex_args = ([1, 2, 3], {"key": "value"})
        complex_kwargs = {"c": {"nested": "dict"}}

        flattened = flatten_args(params, complex_args, complex_kwargs)

        expected = [
            DUMMY_TYPE,
            [1, 2, 3],
            DUMMY_TYPE,
            {"key": "value"},
            "c",
            {"nested": "dict"},
        ]
        assert flattened == expected

    def test_invalid_args_raises_error(self):
        """Test that invalid arguments raise TypeError during flattening."""

        def test_func(a, b):
            return a + b

        params = extract_signature(test_func)
        with pytest.raises(TypeError):
            flatten_args(params, (1, 2, 3), {})  # Too many args


class TestRecoverArgs:
    """Tests for the recover_args utility function."""

    def test_only_positional_args(self):
        """Test recovering only positional arguments."""
        flattened = [DUMMY_TYPE, 1, DUMMY_TYPE, 2, DUMMY_TYPE, 3]
        args, kwargs = recover_args(flattened)

        assert args == [1, 2, 3]
        assert kwargs == {}

    def test_only_keyword_args(self):
        """Test recovering only keyword arguments."""
        flattened = ["a", 10, "b", 20, "c", 30]
        args, kwargs = recover_args(flattened)

        assert args == []
        assert kwargs == {"a": 10, "b": 20, "c": 30}

    def test_mixed_args(self):
        """Test recovering mixed positional and keyword arguments."""
        flattened = [DUMMY_TYPE, 1, DUMMY_TYPE, 2, "c", 3]
        args, kwargs = recover_args(flattened)

        assert args == [1, 2]
        assert kwargs == {"c": 3}

    def test_empty_flattened(self):
        """Test recovering from empty flattened list."""
        flattened = []
        args, kwargs = recover_args(flattened)

        assert args == []
        assert kwargs == {}

    def test_complex_types(self):
        """Test recovering complex argument types."""
        flattened = [
            DUMMY_TYPE,
            [1, 2, 3],
            DUMMY_TYPE,
            {"key": "value"},
            "c",
            {"nested": "dict"},
        ]
        args, kwargs = recover_args(flattened)

        assert args == [[1, 2, 3], {"key": "value"}]
        assert kwargs == {"c": {"nested": "dict"}}

    def test_invalid_odd_length(self):
        """Test error handling for odd-length flattened list."""
        flattened = [DUMMY_TYPE, 1, "key"]  # Odd length
        with pytest.raises(
            AssertionError, match="Flattened arguments need to be even-numbered"
        ):
            recover_args(flattened)

    def test_preserve_order(self):
        """Test that argument order is preserved during flatten/recover."""

        def test_func(a, b, c, d, e):
            return a + b + c + d + e

        params = extract_signature(test_func)
        original_args = (1, 2, 3)
        original_kwargs = {"d": 4, "e": 5}

        flattened = flatten_args(params, original_args, original_kwargs)
        recovered_args, recovered_kwargs = recover_args(flattened)

        assert recovered_args == [1, 2, 3]
        assert recovered_kwargs == {"d": 4, "e": 5}


class TestIntegration:
    """Integration tests for signature utilities working together."""

    def test_complete_workflow(self):
        """Test complete workflow from function to flatten/recover."""

        def test_func(x: int, y: str = "default", z: Optional[Any] = None):
            return f"{x}_{y}_{z}"

        # Extract signature
        params = extract_signature(test_func)
        assert len(params) == 3

        # Validate arguments
        args = (42, "hello")
        kwargs = {"z": [1, 2, 3]}
        validate_args(params, args, kwargs)

        # Flatten arguments
        flattened = flatten_args(params, args, kwargs)
        expected = [DUMMY_TYPE, 42, DUMMY_TYPE, "hello", "z", [1, 2, 3]]
        assert flattened == expected

        # Recover arguments
        recovered_args, recovered_kwargs = recover_args(flattened)
        assert recovered_args == list(args)
        assert recovered_kwargs == kwargs

    def test_method_workflow_with_ignore_first(self):
        """Test complete workflow for class methods with ignore_first=True."""

        class TestClass:
            def test_method(self, a: int, b: str = "test"):
                return f"{a}_{b}"

        # Extract signature ignoring 'self'
        params = extract_signature(TestClass.test_method, ignore_first=True)
        assert len(params) == 2
        assert params[0].name == "a"
        assert params[1].name == "b"

        # Validate and flatten
        args = (100,)
        kwargs = {"b": "custom"}
        validate_args(params, args, kwargs)
        flattened = flatten_args(params, args, kwargs)

        # Recover and verify
        recovered_args, recovered_kwargs = recover_args(flattened)
        assert recovered_args == list(args)
        assert recovered_kwargs == kwargs

    def test_varargs_kwargs_workflow(self):
        """Test workflow with functions that have *args and **kwargs."""

        def test_func(a, b=10, *args, **kwargs):
            return a + b + sum(args) + sum(kwargs.values())

        params = extract_signature(test_func)

        # Test with extra positional and keyword arguments
        args = (1, 2, 3, 4, 5)
        kwargs = {"extra1": 10, "extra2": 20}

        validate_args(params, args, kwargs)
        flattened = flatten_args(params, args, kwargs)
        recovered_args, recovered_kwargs = recover_args(flattened)

        assert recovered_args == list(args)
        assert recovered_kwargs == kwargs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
