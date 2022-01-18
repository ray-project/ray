# Ray Documentation

Repository for documentation of the Ray project, hosted at [docs.ray.io](https://docs.ray.io).

## Installation

To build the documentation, make sure you have `ray` installed first.
For building the documentation locally, allowing for faster builds, install the _development_ dependencies:

```bash
pip install -r requirements-dev.txt  # development dependencies for faster builds
```

If you want to reproduce the production environment and its build, install the _production_ dependencies instead:


```bash
pip install -r requirements-doc.txt  # readthedocs.org dependencies
````

## Building the documentation

To compile the documentation and open it locally, run the following command from this directory.

```bash
make html && open _build/html/index.html
```

To build the documentation more strictly, by treating warnings as errors, run the following command
(the `-W` flag is required for this to work):

```bash
sphinx-build -W -b html -d _build/doctrees source _build/html
```

## Checking for broken links

To check if there are broken links, run the following (we are currently not running this
in the CI since there are false positives).

```bash
make linkcheck
```

## Running doctests

To run tests for examples shipping with docstrings in Python files, run the following command:

```shell
make doctest
```