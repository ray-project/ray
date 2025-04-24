import pytest
from ray.serve._private.utils import get_component_file_name
from ray.serve._private.common import ServeComponentType

def test_get_component_file_name_basic():
    """Test basic component file name generation without special characters."""
    result = get_component_file_name(
        component_name="test_component",
        component_id="123",
        component_type=None,
    )
    assert result == "test_component_123"

def test_get_component_file_name_with_special_characters():
    """Test component file name generation with special characters."""
    test_cases = [
        ("test@component", "test_component"),
        ("test#123", "test_123"),
        ("component/name", "component_name"),
        ("component.name", "component_name"),
        ("component!name", "component_name"),
        ("component$name", "component_name"),
        ("component%name", "component_name"),
        ("component^name", "component_name"),
        ("component&name", "component_name"),
        ("component*name", "component_name"),
        ("component(name", "component_name"),
        ("component)name", "component_name"),
        ("component+name", "component_name"),
        ("component=name", "component_name"),
        ("component{name", "component_name"),
        ("component}name", "component_name"),
        ("component[name", "component_name"),
        ("component]name", "component_name"),
        ("component:name", "component_name"),
        ("component;name", "component_name"),
        ("component'name", "component_name"),
        ('component"name', "component_name"),
        ("component<name", "component_name"),
        ("component>name", "component_name"),
        ("component,name", "component_name"),
        ("component?name", "component_name"),
        ("component|name", "component_name"),
        ("component\\name", "component_name"),
    ]

    for input_name, expected_name in test_cases:
        result = get_component_file_name(
            component_name=input_name,
            component_id="123",
            component_type=None,
        )
        assert result == f"{expected_name}_123"

def test_get_component_file_name_with_component_type():
    """Test component file name generation with different component types."""
    component_types = [
        ServeComponentType.DEPLOYMENT,
        ServeComponentType.REPLICA,
        ServeComponentType.HTTP_PROXY,
    ]

    for component_type in component_types:
        result = get_component_file_name(
            component_name="test@component",
            component_id="123",
            component_type=component_type,
        )
        if component_type == ServeComponentType.REPLICA:
            assert result == "test_component_123"
        else:
            assert result == f"{component_type.value}_test_component_123"

def test_get_component_file_name_with_suffix():
    """Test component file name generation with suffix."""
    result = get_component_file_name(
        component_name="test@component",
        component_id="123",
        component_type=None,
        suffix=".log",
    )
    assert result == "test_component_123.log"

def test_get_component_file_name_with_special_characters_in_id():
    """Test component file name generation with special characters in component_id."""
    result = get_component_file_name(
        component_name="test_component",
        component_id="123@456",
        component_type=None,
    )
    assert result == "test_component_123_456"

def test_get_component_file_name_with_multiple_special_characters():
    """Test component file name generation with multiple consecutive special characters."""
    result = get_component_file_name(
        component_name="test@@@component",
        component_id="123###456",
        component_type=None,
    )
    assert result == "test_component_123_456"

def test_get_component_file_name_with_unicode_characters():
    """Test component file name generation with unicode characters."""
    result = get_component_file_name(
        component_name="test✓component",
        component_id="123✓456",
        component_type=None,
    )
    assert result == "test_component_123_456" 