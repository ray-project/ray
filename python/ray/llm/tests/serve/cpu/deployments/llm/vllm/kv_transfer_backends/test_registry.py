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

    def test_double_registration_raises(self):
        """Test that double registration raises ValueError."""
        registry = ComponentRegistry("test_category")
        test_class1 = type("TestClass1", (), {})
        test_class2 = type("TestClass2", (), {})

        registry.register("test_component", test_class1)

        with pytest.raises(ValueError, match="already registered"):
            registry.register("test_component", test_class2)

        # Verify original registration is still intact
        assert registry.get("test_component") == test_class1

    def test_reregister_after_unregister(self):
        """Test that unregistering allows re-registration."""
        registry = ComponentRegistry("test_category")
        test_class1 = type("TestClass1", (), {})
        test_class2 = type("TestClass2", (), {})

        registry.register("test_component", test_class1)
        registry.unregister("test_component")
        registry.register("test_component", test_class2)

        assert registry.get("test_component") == test_class2

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

    def test_unregister(self):
        """Test unregistering a component."""
        registry = ComponentRegistry("test_category")
        test_class = type("TestClass", (), {})

        # Register and verify it exists
        registry.register("test_component", test_class)
        assert registry.contains("test_component")

        # Unregister and verify it's removed
        registry.unregister("test_component")
        assert not registry.contains("test_component")

        # Verify get raises ValueError
        with pytest.raises(ValueError, match="not found"):
            registry.get("test_component")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
