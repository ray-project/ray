---
jupytext:
    text_representation:
        extension: .md
        format_name: myst
kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

(docs-contribute)=

# Contributing to the Ray Documentation

There are many ways to contribute to Ray, and we're always looking for new contributors.
Even if you just want to fix a typo or expand on a section, please feel free to do so!

TODO link to easy docs tickets and good first issues. 

This document walks you through everything you need to do to get started.

## Building the Ray documentation

If you want to contribute to the Ray documentation, you'll need a way to build it.
You don't have to build Ray itself, which is a bit more involved.
Just clone the Ray repository and change into the `ray/docs` directory.

```shell
git clone git@github.com:ray-project/ray.git
cd ray/docs
```

To install the documentation dependencies, run the following command:

```shell
pip install -r requirements-doc.txt
```

Additionally, it's best if you install the dependencies for our linters with

```shell
pip install -r ../python/requirements_linters.txt
```

so that you can make sure your changes comply with our style guide.
Building the documentation is done by running the following command:

```shell
make html
```

which will build the documentation into the `_build` directory.
After the build finishes, you can simply open the `_build/html/index.html` file in your browser.
It's considered good practice to check the output of your build to make sure everything is working as expected.

## The basics of our build system

The Ray documentation is built using the [`sphinx`](https://www.sphinx-doc.org/) build system.
We're using the [Sphinx Book Theme](https://github.com/executablebooks/sphinx-book-theme) from the
[executable books project](https://github.com/executablebooks).

That means that you can write Ray documentation in either Sphinx's native 
[reStructuredText (rST)](https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html) or in
[Markedly Structured Text (MyST)](https://myst-parser.readthedocs.io/en/latest/).
The two formats can be converted to each other, so the choice is up to you.
Having said that, it's important to know that MyST is
[common markdown compliant](https://myst-parser.readthedocs.io/en/latest/syntax/reference.html#commonmark-block-tokens).
If you intend to add a new document, we recommend starting from an `.md` file.

The Ray documentation also fully supports executable formats like [Jupyter Notebooks](https://jupyter.org/).
Many of our examples are notebooks with [MyST markdown cells](https://myst-nb.readthedocs.io/en/latest/index.html).
In fact, this very document you're reading _is_ a notebook.
You can check this for yourself by either downloading the `.ipynb` file,
or directly launching this notebook into either Binder or Google Colab in the top navigation bar.

## What to contribute?

If you take Ray Tune as an example, you can see that our documentation is made up from

- [a project landing page](https://docs.ray.io/en/master/tune/index.html),
- [a getting started guide](https://docs.ray.io/en/master/tune/getting-started.html),
- [a key concepts page](https://docs.ray.io/en/master/tune/key-concepts.html),
- [user guides for key features](https://docs.ray.io/en/master/tune/tutorials/overview.html),
- [practical examples](https://docs.ray.io/en/master/tune/examples/index.html),
- [a detailed FAQ](https://docs.ray.io/en/master/tune/faq.html),
- [and API references](https://docs.ray.io/en/master/tune/api_docs/overview.html).

This structure is reflected in the
[Ray documentation source code](https://github.com/ray-project/ray/tree/master/doc/source/tune) as well, so you
should have no problem finding what you're looking for.
All other Ray projects share a similar structure, but depending on the project there might be minor differences.

Each type of documentation listed above has its own purpose, but at the end our documentation
comes down to _two types_ of documents:

- Markup documents, written in MyST or rST. If you don't have a lot of (executable) code to contribute or
  use more complex features such as
  [tabbed content blocks](https://docs.ray.io/en/master/ray-core/walkthrough.html#starting-ray), this is the right
  choice. Most of the documents in Ray Tune are written in this 
- Notebooks, written in `.ipynb` format. All Tune examples are written as notebooks. These notebooks render in
  the browser like `.md` or `.rst` files, but have the added benefit of adding launch buttons to the top of the
  document, so that users can run the code themselves in either Binder or Google Colab.

### Fixing typos or adding tests

### Expanding on a section