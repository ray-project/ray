"""Unit tests for ComponentRegistry."""

import sys

import pytest

from ray.llm._internal.serve.utils.registry import ComponentRegistry, get_registry


class TestComponentRegistry:
    """Test suite for ComponentRegistry."""

    def test_register_and_get_direct_class(self):
        """Test registering and retrieving a class directly."""
        registry = ComponentRegistry("test_category")
        test_class = type("TestClass", (), {})

        registry.register("test_component", test_class)
        assert registry.contains("test_component")
        retrieved = registry.get("test_component")
        assert retrieved == test_class

    def test_register_and_get_module_path(self):
        """Test registering and retrieving via module path."""
        registry = ComponentRegistry("test_category")

        registry.register(
            "test_component",
            "ray.llm._internal.serve.utils.registry:ComponentRegistry",
        )
        assert registry.contains("test_component")
        retrieved = registry.get("test_component")
        assert retrieved == ComponentRegistry

    def test_get_nonexistent_component_raises(self):
        """Test that getting a non-existent component raises ValueError."""
        registry = ComponentRegistry("test_category")

        with pytest.raises(ValueError, match="not found"):
            registry.get("nonexistent")

    def test_invalid_string_format_raises(self):
        """Test that registering with invalid string format raises ValueError."""
        registry = ComponentRegistry("test_category")

        with pytest.raises(ValueError, match="Invalid format"):
            registry.register("test_comp", "invalid_format_no_colon")

    def test_get_registry_singleton(self):
        """Test that get_registry returns the same instance for the same category."""
        registry1 = get_registry("test_category")
        registry2 = get_registry("test_category")

        assert registry1 is registry2
        assert registry1.category == "test_category"

    def test_get_registry_different_categories(self):
        """Test that get_registry returns different instances for different categories."""
        registry1 = get_registry("category1")
        registry2 = get_registry("category2")

        assert registry1 is not registry2
        assert registry1.category == "category1"
        assert registry2.category == "category2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

