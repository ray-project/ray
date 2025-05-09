"""Configuration for pytest."""

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup environment variables and configurations for testing."""
    # Save current working directory
    original_dir = os.getcwd()

    # Set any environment variables needed for testing
    os.environ["PYTHONPATH"] = os.getcwd()

    yield

    # Restore original working directory
    os.chdir(original_dir)
