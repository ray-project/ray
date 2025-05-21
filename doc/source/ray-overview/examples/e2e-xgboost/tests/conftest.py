"""Configuration for pytest."""

import os
import sys
import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """
    Setup the Python path to allow importing 'dist_xgboost' during tests
    without needing to be installed, and restore the original state afterward.
    Also, handles current working directory.
    """
    # Save current working directory and original sys.path
    original_cwd = os.getcwd()
    original_sys_path = list(sys.path)  # Make a copy for restoration

    # Calculate the path to the project's base directory.
    conftest_file_path = os.path.abspath(__file__)
    tests_dir = os.path.dirname(conftest_file_path)
    project_base_dir = os.path.dirname(tests_dir)

    # Enable imports like 'from dist_xgboost.serve import ...'
    if project_base_dir not in sys.path:
        sys.path.insert(0, project_base_dir)

    yield

    # Teardown
    os.chdir(original_cwd)
    sys.path[:] = original_sys_path
