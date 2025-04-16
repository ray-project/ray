# Notebook Tests

This directory contains pytest tests for validating the Jupyter notebooks in the repository.

## Running the Tests

To run the notebook tests:

```bash
# Install development dependencies
pip install -r requirements_dev.txt

# Run all tests
pytest

# Run specific test file
pytest tests/test_notebooks.py


# Run only the ordered workflow test
pytest tests/test_notebooks.py::test_notebook_workflow -v
```

## How It Works

The tests convert each Jupyter notebook to a Python script, removing cells with the `remove-cell-ci` tag, and then execute the script to ensure it runs without errors.

The tests execute in a specific order since each stage of the tutorial depends on the last.
