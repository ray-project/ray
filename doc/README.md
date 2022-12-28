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
make develop && open _build/html/index.html
```

> **_NOTE:_**  The above command is for development. To reproduce build failures from the
> CI, you should use `make html` which is the same as `make develop` but treats warnings as errors.

## Building just one sub-project

Often your changes in documentation just concern one sub-project, such as Tune or Train.
To build just this one sub-project, and ignore the rest
(leading to build warnings due to broken references etc.), run the following command:

```shell
DOC_LIB=<project> sphinx-build -b html -d _build/doctrees  source _build/html
```
where `<project>` is the name of the sub-project and can be any of the docs projects in the `source/`
directory either called `tune`, `rllib`, `train`, `cluster`, `serve`, `data` or the ones starting
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
RAY_MOCK_MODULES=0 make doctest
```

## Adding examples as MyST Markdown Notebooks

You can now add [executable notebooks](https://myst-nb.readthedocs.io/en/latest/use/markdown.html) to this project,
which will get built into the documentation.
An [example can be found here](./source/serve/tutorials/rllib.md).
By default, building the docs with `make develop` will not run those notebooks.
If you set the `RUN_NOTEBOOKS` environment variable to `"cache"`, each notebook cell will be run when you build the
documentation, and outputs will be cached into `_build/.jupyter_cache`.

```bash
RUN_NOTEBOOKS="cache" make develop
```

To force re-running the notebooks, use `RUN_NOTEBOOKS="force"`.

Using caching, this means the first time you build the documentation, it might take a while to run the notebooks.
After that, notebook execution is only triggered when you change the notebook source file.

The benefits of working with notebooks for examples are that you don't separate the code from the documentation, but can still easily smoke-test the code.

## Adding Markdown docs from external (ecosystem) repositories

In order to avoid a situation where duplicate documentation files live in both the docs folder
in this repository and in external repositories of ecosystem libraries (eg. xgboost-ray), you can
specify Markdown files that will be downloaded from other GitHub repositories during the build process.

In order to do that, simply edit the `EXTERNAL_MARKDOWN_FILES` list in `source/custom_directives.py`
using the format in the comment. Before build process, the specified files will be downloaded, preprocessed
and saved to given paths. The build process will then proceed as normal.

While both GitHub Markdown and MyST are supersets of Common Markdown, there are differences in syntax.
Furthermore, some contents such as Sphinx headers are not desirable to be displayed on GitHub.
In order to deal with this, simple preprocessing is performed to allow for differences
in rendering on GitHub and in docs. You can use two commands (`$UNCOMMENT` and `$REMOVE`/`$END_REMOVE`)
in the Markdown file, specified in the following way:

### `$UNCOMMENT`

GitHub:

```html
<!--$UNCOMMENTthis will be uncommented--> More text
```

In docs, this will become:

```html
this will be uncommented More text
```

### `$REMOVE`/`$END_REMOVE`

GitHub:

```html
<!--$REMOVE-->This will be removed<!--$END_REMOVE--> More text
```

In docs, this will become:

```html
More text
```

Please note that the parsing is extremely simple (regex replace) and will not support nesting.

## Testing changes locally

If you want to run the preprocessing locally on a specific file (to eg. see how it will render after docs have been built), run `source/preprocess_github_markdown.py PATH_TO_MARKDOWN_FILE PATH_TO_PREPROCESSED_MARKDOWN_FILE`. Make sure to also edit `EXTERNAL_MARKDOWN_FILES` in `source/custom_directives.py` so that your file does not get overwritten by one downloaded form GitHub.
