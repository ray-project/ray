# Ray Documentation

Repository for documentation of the Ray project, hosted at [docs.ray.io](https://docs.ray.io).

## Installation

To build the documentation, make sure you have `ray` installed first.
For building the documentation locally install the following dependencies:

```bash
pip install -r requirements-doc.txt
```

## Building the documentation

To compile the documentation and open it locally, run the following command from this directory.

```bash
make html && open _build/html/index.html
```

## Building just one sub-project

Often your changes in documentation just concern one sub-project, such as Tune or Train.
To build just this one sub-project, and ignore the rest (leading to build warnings due to broken references etc.), run the following command:

```shell
DOC_LIB=<project> sphinx-build -b html -d _build/doctrees  source _build/html
```
where `<project>` is the name of the sub-project and can be any of the docs projects in the `source/`
directory either called `tune`, `rllib`, `train`, `cluster`, `serve`, `raysgd`, `data` or the ones starting
with `ray-`, e.g. `ray-observability`.

## Announcements and includes

To add new announcements and other messaging to the top or bottom of a documentation page,
check the `_includes` folder first to see if the message you want is already there (like "get help"
or "we're hiring" etc.)
If not, add the template you want and include it accordingly, i.e. with

```markdown
.. include:: /_includes/<my-announcement>
```

This ensures consistent messaging across documentation pages.

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