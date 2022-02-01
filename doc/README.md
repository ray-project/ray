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

## Adding examples as MyST Markdown Notebooks

You can now add [executable notebooks](https://myst-nb.readthedocs.io/en/latest/use/markdown.html) to this project,
which will get built into the documentation.
An [example can be found here](./source/serve/tutorials/rllib.md).

Each notebook cell will be run when you build the documentation, but outputs will be cached into `_build/.jupyter_cache`.
This means the first time you build the documentation, it might take a while to run the notebooks.
After that, notebook execution is only triggered when you change the notebook source file.

The benefits of working with notebooks for examples are that you don't separate the code from the documentation,
but can still easily smoke-test the code.
You also get nice notebook output formatting (no need to copy script output into `md` or `rst` files) and plots.
These markdown notebooks can also seamlessly converted to and from `ipynb` notebooks or python scripts.