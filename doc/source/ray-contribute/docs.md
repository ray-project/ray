---
myst:
  html_meta:
    description: "How to contribute to the Ray documentation: building the docs locally, the Google developer style guide enforced by Vale, and the conventions for writing and previewing pages. Use this when editing or adding Ray documentation."
---

# Contributing to the Ray documentation

There are many ways to contribute to the Ray documentation, and we're always looking for new contributors. Even if you want to fix a typo or expand a section, feel free to do so!

This document walks you through everything you need to do to get started.


## Editorial style

The Ray documentation follows a house style guide that covers voice, word choice, sentence structure, headings, lists, links, and formatting. Read it before you write or edit a page. See {ref}`documentation-style`.

The house guide builds on the [Google developer documentation style guide](https://developers.google.com/style), which is the fallback for anything the house guide doesn't cover. Vale enforces an automated subset of the Google guide in CI. For more information, see [How to use Vale](vale).

## Building the Ray documentation

If you want to contribute to the Ray documentation, you need a way to build it. Don't install Ray in the environment you plan to use to build documentation. The requirements for the docs build system are generally not compatible with those you need to run Ray itself.

Follow these instructions to build the documentation:

### Fork Ray

1. {ref}`Fork the Ray repository <fork-ray-repo>`.
2. {ref}`Clone the forked repository <fork-ray-repo>` to your local machine.

Next, go to the `ray/doc` directory:

```shell
cd ray/doc
```

### Install dependencies

If you haven't done so already, create a Python environment separate from the one you use to build and run Ray. Use Python 3.11 to match the version Read the Docs builds with. For example, if you're using `conda`:
```shell
conda create -n docs python=3.11
```
Next, activate the Python environment you're using (for example, venv or conda). With `conda`, this would be:
```shell
conda activate docs
```
Install the documentation dependencies with the following command:

```shell
pip install -r requirements-doc.lock.txt
```

Don't use `-U` in this step. `requirements-doc.lock.txt` is a lock file that pins the exact versions of all the required dependencies.

### Build documentation

Before building, clean your environment by running:
```shell
make clean
```

Choose from the following two options to build documentation locally:

- Incremental build
- Full build

#### 1. Incremental build with global cache and live rendering

To use this option, you can run:
```shell
make local
```

We recommend this option if you need to make frequent, small, uncomplicated changes such as editing text or adding content within existing files.

In this approach, Sphinx only builds the changes you made in your branch compared to your last pull from upstream master. The rest of the doc is cached with pre-built doc pages from your last commit from upstream. For every new commit pushed to Ray, CI builds all the documentation pages from that commit and stores them on S3 as cache.

The build first traces your commit tree to find the latest commit that CI already cached on S3. Once the build finds the commit, it fetches the corresponding cache from S3 and extracts it into the `doc/` directory. Simultaneously, CI tracks all the files that have changed from that commit to current `HEAD`, including any unstaged changes.

Sphinx then rebuilds only the pages that your changes affect, leaving the rest untouched from the cache.

When the build finishes, the doc page automatically opens in your browser. If you make a change in the `doc/` directory, Sphinx automatically rebuilds and reloads the page. Stop it with `Ctrl+C`.

For more complicated changes that involve adding or removing files, always use `make develop` first. Then use `make local` to iterate on the cache that `make develop` produces.

#### 2. Full build from scratch

In the full build option, Sphinx rebuilds all files in the `doc/` directory, ignoring all cache and saved environment. Because of this behavior, you get a clean build, but it's much slower.

```shell
make develop
```

Find the documentation build in the `_build` directory. After the build finishes, open the `_build/html/index.html` file in your browser. It's good practice to check the output of your build to make sure everything works as expected.

Before committing any changes, run the [linter](getting-involved.md#lint-and-formatting) with `pre-commit run` from the `doc` folder to make sure your changes are formatted correctly.

### Verify a Read the Docs-faithful build

`make local` and `make develop` are for fast iteration while you author. Before you push, verify your changes the way Read the Docs builds them, so you catch failures locally instead of waiting on a Read the Docs build:

```shell
make rtd-build
```

`make rtd-build` reproduces the full build Read the Docs runs from a fresh checkout, with `fail_on_warning` enabled, so any Sphinx warning fails the build exactly as it does on Read the Docs. It first runs a preflight check that your environment matches Read the Docs — the Python version Read the Docs builds with (3.11) and the dependency versions pinned in `requirements-doc.lock.txt` — and stops with an explanation if it finds drift. Run that check on its own at any time with:

```shell
make rtd-doctor
```

A full build matters most when you add, remove, or rename files. The incremental `make local` build reuses cached pages, so a rename that breaks a cross-reference in an otherwise-unchanged page can pass locally and only fail on Read the Docs. `make rtd-build` always does a full build, so it catches these.

If you intend to build on an environment that doesn't match Read the Docs, run `make rtd-build RTD_DOCTOR_ARGS=--warn-only` to downgrade the preflight check from an error to a warning.

### Code completion and other developer tooling

If you find yourself working with documentation often, you might find the [esbonio](https://github.com/swyddfa/esbonio) language server useful. Esbonio provides context-aware syntax completion, definitions, diagnostics, document links, and other information for RST documents. If you're unfamiliar with [language servers](https://en.wikipedia.org/wiki/Language_Server_Protocol), they're important pieces of a modern developer's toolkit. If you've used `pylance` or `python-lsp-server` before, you'll know how useful these tools can be.

Esbonio also provides a VS Code extension that includes a live preview. Install the `esbonio` VS Code extension to start using the tool:

![The esbonio extension in VS Code](esbonio.png)

As an example of Esbonio's autocompletion capabilities, you can type `..` to pull up an autocomplete menu for all RST directives:

![VS Code autocomplete menu showing RST directives](completion.png)

Esbonio also works with neovim. [See the lspconfig repository for installation instructions](https://github.com/neovim/nvim-lspconfig/blob/master/doc/server_configurations.md#esbonio).


## The basics of our build system

The Ray documentation is built with the [`sphinx`](https://www.sphinx-doc.org/) build system. We use the [PyData Sphinx Theme](https://pydata-sphinx-theme.readthedocs.io/en/stable/) for the documentation.

We use [`myst-parser`](https://myst-parser.readthedocs.io/en/latest/) so Ray documentation supports both Sphinx's native [reStructuredText (rST)](https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html) and [Markedly Structured Text (MyST)](https://myst-parser.readthedocs.io/en/latest/). New pages must be MyST Markdown (`.md`). A lint check rejects newly added `.rst` files, though edits to existing `.rst` files are fine. MyST is [CommonMark-compliant](https://myst-parser.readthedocs.io/en/latest/syntax/reference.html#commonmark-block-tokens), and you can convert between the two formats, so existing rST pages are straightforward to work with.

The Ray documentation also fully supports executable formats such as [Jupyter Notebooks](https://jupyter.org/). Many of our examples are notebooks with [MyST markdown cells](https://myst-nb.readthedocs.io/en/latest/index.html).

## What to contribute?

If you take Ray Tune as an example, you can see that our documentation consists of several types of documentation, all of which you can contribute to:

- [a project landing page](https://docs.ray.io/en/latest/tune/index.html),
- [a getting started guide](https://docs.ray.io/en/latest/tune/getting-started.html),
- [a key concepts page](https://docs.ray.io/en/latest/tune/key-concepts.html),
- [user guides for key features](https://docs.ray.io/en/latest/tune/tutorials/overview.html),
- [practical examples](https://docs.ray.io/en/latest/tune/examples/index.html),
- [a detailed FAQ](https://docs.ray.io/en/latest/tune/faq.html),
- [and API references](https://docs.ray.io/en/latest/tune/api/api.html).

This structure is reflected in the [Ray documentation source code](https://github.com/ray-project/ray/tree/master/doc/source/tune) as well, so you should have no problem finding what you're looking for. All other Ray projects share a similar structure, but depending on the project there might be minor differences.

Each type of documentation listed above has its own purpose, but ultimately our documentation comes down to _two types_ of documents:

- Markup documents, written in MyST or rST. If you don't have a lot of (executable) code to contribute or use more complex features such as [tabbed content blocks](https://docs.ray.io/en/latest/ray-core/walkthrough.html#starting-ray), this is the right choice. Most of the documents in Ray Tune are written in this way, for instance the [key concepts](https://github.com/ray-project/ray/blob/master/doc/source/tune/key-concepts.rst) or [API documentation](https://github.com/ray-project/ray/blob/master/doc/source/tune/api/api.rst).
- Notebooks, written in `.ipynb` format. All Tune examples are written as notebooks. These notebooks render in the browser like `.md` or `.rst` files, but have the added benefit that users can run the code themselves.

## Fixing typos and improving explanations

If you spot a typo in any document, or think an explanation isn't clear enough, consider opening a pull request. In this scenario, run the linter as described above and submit your pull request.

## Adding API references

We use [Sphinx's autodoc extension](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html) to generate our API documentation from our source code. If we're missing a reference to a function or class, consider adding it to the document in question.

For example, here's how you can add a function or class reference using `autofunction` and `autoclass`:

```markdown
.. autofunction:: ray.tune.register_env

.. autoclass:: ray.tune.Tuner
```

These directives appear throughout the API reference, such as the [Tune API documentation](https://github.com/ray-project/ray/tree/master/doc/source/tune/api), which you can look at for reference.

If you want to change the content of the API documentation, you must edit the function or class signatures directly in the source code. For example, in the above `autofunction` call, to change the API reference for `ray.tune.register_env`, you would edit its docstring in the [source file](https://github.com/ray-project/ray/blob/master/python/ray/tune/registry.py).

To show the usage of APIs, it's important to have small usage examples embedded in the API documentation. These should be self-contained and run out of the box, so a user can copy and paste them into a Python interpreter and play around with them. For example, if applicable, they should point to example data. Users often rely on these examples to build their applications. To learn more about writing examples, read [How to write code snippets](writing-code-snippets).

(api-ref-build-behavior)=

### How the docs build renders your API signatures

The API reference is generated from your source code: autodoc imports the module to read its signatures and docstrings. Two build behaviors affect what you write in code, even if you never build the docs yourself.

**Heavy dependencies are mocked, so keep your imports safe.** The docs build installs only a light dependency set, not Ray's full runtime. Heavy or optional libraries such as `torch`, `tensorflow`, and `pandas` are replaced by mock objects, listed in `autodoc_mock_imports` in `doc/source/conf.py`, so autodoc can import your module without importing those libraries. If your module imports a heavy dependency at import time and that library isn't mocked, the API-ref build fails. Note that Sphinx's autodoc sets `typing.TYPE_CHECKING` to `True` during the build to resolve type annotations, so imports guarded by `if TYPE_CHECKING:` will still be executed and can cause failures if not mocked. A mock can also stand in for an object incorrectly and abort the whole module import, which surfaces as a confusing, unrelated error. To avoid both, import heavy dependencies lazily inside the function or method that needs them rather than at module top level. If you add a public API that puts a new heavy dependency in a signature, add that library to `autodoc_mock_imports`.

**Type annotations link to external docs through intersphinx.** When a public signature is annotated with a type from an external library, such as `numpy.ndarray` or `torch.Tensor`, the build turns it into a link to that library's own documentation using the `intersphinx_mapping` in `doc/source/conf.py`. The link resolves only if the library is in that mapping. If you add a public API whose signature references a new external library and you want its types linked, add the library to `intersphinx_mapping` (and, per the point above, usually to `autodoc_mock_imports` too). Annotations that don't resolve render as plain text; they don't fail the build.

## Adding code to an `.rST` or `.md` file

Modifying text in an existing documentation file is easy, but you need to be careful when it comes to adding code. The reason is that we want to ensure every code snippet in our documentation is tested. This requires us to have a process for including and testing code snippets in documents. To learn how to write testable code snippets, read [How to write code snippets](writing-code-snippets).

```python
from ray import train


def objective(x, a, b):  # Define an objective function.
    return a * (x ** 0.5) + b


def trainable(config):  # Pass a "config" dictionary into your trainable.

    for x in range(20):  # "Train" for 20 iterations and compute intermediate scores.
        score = objective(x, config["a"], config["b"])

        train.report({"score": score})  # Send the score to Tune.
```

This code is imported by `literalinclude` from a file called `doc_code/key_concepts.py`. Every Python file in the `doc_code` directory is automatically tested by our CI system, but make sure to run scripts that you change (or new scripts) locally first. You don't need to run the testing framework locally.

In rare situations, when you're adding _obvious_ pseudo-code to demonstrate a concept, it's OK to add it literally into your `.rst` or `.md` file, for example, using a `.. code-cell:: python` directive. But if your code is supposed to run, it needs to be tested.

## Creating a new document from scratch

Sometimes you might want to add a completely new document to the Ray documentation, such as a new user guide or a new example.

For this to work, you must add the new document explicitly to a parent document's toctree, which determines the structure of the Ray documentation. See [the Sphinx documentation](https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html#directive-toctree) for more information.

Depending on the type of document you're adding, you might also have to make changes to an existing overview page that curates the list of documents in question. For instance, for Ray Tune each user guide is added to the [user guide overview page](https://docs.ray.io/en/latest/tune/tutorials/overview.html) as a panel, and the same goes for [all Tune examples](https://docs.ray.io/en/latest/tune/examples/index.html). Always check the structure of the Ray sub-project whose documentation you're working on to see how to integrate it within the existing structure. In some cases you may need to choose an image for the panel. Images are in `doc/source/images`.

## Creating a notebook example

To add a new executable example to the Ray documentation, you can start from our [MyST notebook template](https://github.com/ray-project/ray/blob/master/doc/source/_templates/template.md) or [Jupyter notebook template](https://github.com/ray-project/ray/blob/master/doc/source/_templates/template.ipynb). You could also download the document you're reading right now and start modifying it. Click the download button at the top of this page to get the `.ipynb` file. All the example notebooks in Ray Tune are automatically tested by our CI system, provided you place them in the [`examples` folder](https://github.com/ray-project/ray/tree/master/doc/source/tune/examples). If you have questions about how to test your notebook when contributing to other Ray sub-projects, ask in [the Ray community Slack](https://www.ray.io/join-slack) or directly on GitHub when opening your pull request.

To work from an existing example, look at the [Ray Tune Hyperopt example (`.ipynb`)](https://github.com/ray-project/ray/blob/master/doc/source/tune/examples/hyperopt_example.ipynb) or the [Ray Serve guide for text classification (`.md`)](https://github.com/ray-project/ray/blob/master/doc/source/serve/tutorials/text-classification.md). We recommend that you start with an `.md` file and convert it to an `.ipynb` notebook at the end of the process. We'll walk you through this process below.

What makes these notebooks different from other documents is that they combine code and text in one document, and you can launch them in the browser. We also make sure our CI system tests them before we add them to our documentation. To make this work, notebooks need to define a _kernel specification_ to tell a notebook server how to interpret and run the code. For instance, here's the kernel specification of a Python notebook:

```markdown
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
```

If you write a notebook in `.md` format, you need this YAML front matter at the top of the file. To add code to your notebook, you can use the `code-cell` directive. Here's an example:

````markdown
```python
from ray.rllib.algorithms.ppo import PPOConfig

# Configure PPO on the CartPole environment.
config = PPOConfig().environment("CartPole-v1")

# Build the algorithm and train it for one iteration.
algo = config.build_algo()
algo.train()
```
````

Putting this markdown block into your document renders as follows in the browser:

```python
from ray.rllib.algorithms.ppo import PPOConfig

# Configure PPO on the CartPole environment.
config = PPOConfig().environment("CartPole-v1")

# Build the algorithm and train it for one iteration.
algo = config.build_algo()
algo.train()
```

### Tags for your notebook

What makes this work is the `:tags: [hide-cell]` directive in the `code-cell`. The reason we suggest starting with `.md` files is that it's much easier to add tags to them, as you've seen. You can also add tags to `.ipynb` files, but you'll need to start a notebook server for that first, which you may not want to do to contribute a piece of documentation.

Apart from `hide-cell`, you also have `hide-input` and `hide-output` tags that hide the input and output of a cell. Also, if you need code that runs in the notebook but you don't want to show it in the documentation, you can use the `remove-cell`, `remove-input`, and `remove-output` tags in the same way.

### Reference section labels

[Reference section labels](https://jupyterbook.org/en/stable/content/references.html#reference-section-labels) are a way to link to specific parts of the documentation from within a notebook. Creating one inside a markdown cell is simple:

```markdown
(my-label)=
# The thing to label
```

Then, you can link it in .rst files with the following syntax:

```rst
See {ref}`the thing that I labeled <my-label>` for more information.
```

### Testing notebooks

Removing cells can be particularly interesting for compute-intensive notebooks. We want you to contribute notebooks that use _realistic_ values, not just toy examples. At the same time, we want our CI system to test our notebooks, and running them shouldn't take too long. To address this, use notebook cells with the parameters you want the users to see first:

````markdown
```{code-cell} python3
num_workers = 8
num_gpus = 2
```
````

which will render as follows in the browser:

```python
num_workers = 8
num_gpus = 2
```

But then in your notebook, you follow that up with a _removed_ cell that won't render, but has much smaller values and makes the notebook run faster:

````markdown
```{code-cell} python3
:tags: [remove-cell]
num_workers = 0
num_gpus = 0
```
````

### Converting markdown notebooks to ipynb

Once you're finished writing your example, you can convert it to an `.ipynb` notebook using `jupytext`:

```shell
jupytext your-example.md --to ipynb
```

In the same way, you can convert `.ipynb` notebooks to `.md` notebooks with `--to myst`. And if you want to convert your notebook to a Python file, for example, to test whether your whole script runs without errors, you can use `--to py` instead.

(vale)=

## How to use Vale
### What is Vale?

[Vale](https://vale.sh/) checks whether your writing adheres to the [Google developer documentation style guide](https://developers.google.com/style). CI enforces it on the Ray Data documentation and the example gallery.

Vale catches typos and grammatical errors. It also enforces stylistic rules such as "use contractions" and "use second person." For the full list of rules, see the [configuration in the Ray repository](https://github.com/ray-project/ray/tree/master/.vale/styles/Google).

### How do you run Vale?

#### How to use the VS Code extension

1. Install Vale. If you use macOS, use Homebrew.

    ```bash
    brew install vale
    ```

    Otherwise, use PyPI.

    ```bash
    pip install vale
    ```

    For more information on installation, see the [Vale documentation](https://vale.sh/docs/vale-cli/installation/).

2. Install the Vale VS Code extension by following these [installation instructions](https://marketplace.visualstudio.com/items?itemName=ChrisChinchilla.vale-vscode).

3. VS Code should show warnings in your code editor and in the "Problems" panel.

    ![Vale warnings in the VS Code Problems panel](../images/vale.png)

#### How to run Vale on the command line

1. Install Vale. If you use macOS, use Homebrew.

    ```bash
    brew install vale
    ```

    Otherwise, use PyPI.

    ```bash
    pip install vale
    ```

    For more information on installation, see the [Vale documentation](https://vale.sh/docs/vale-cli/installation/).

2. Run Vale in your terminal.

    ```bash
    vale doc/source/data/overview.rst
    ```

3. Vale should show warnings in your terminal.

    ```
    ❯ vale doc/source/data/overview.rst

        doc/source/data/overview.rst
        18:1   warning     Try to avoid using              Google.We
                        first-person plural like 'We'.
        18:46  error       Did you really mean             Vale.Spelling
                        'distrbuted'?
        24:10  suggestion  In general, use active voice    Google.Passive
                        instead of passive voice ('is
                        built').
        28:14  warning     Use 'doesn't' instead of 'does  Google.Contractions
                        not'.

    ✖ 1 error, 2 warnings and 1 suggestion in 1 file.
    ```


### How to handle false Vale.Spelling errors

To add custom terminology, complete the following steps:

1. If it doesn't already exist, create a directory for your team in `.vale/styles/Vocab`. For example, `.vale/styles/Vocab/Data`.
2. If it doesn't already exist, create a text file named `accept.txt`. For example, `.vale/styles/Vocab/Data/accept.txt`.
3. Add your term to `accept.txt`. Vale accepts Regex.

For more information, see [Vocabularies](https://vale.sh/docs/topics/vocab/) in the Vale documentation.

### How to handle false Google.WordList errors

Vale errors if you use a word that isn't on [Google's word list](https://developers.google.com/style/word-list).

```
 304:52   error       Use 'select' instead of         Google.WordList
                      'check'.
```

If you want to use the word anyway, modify the appropriate field in the [WordList configuration](https://github.com/ray-project/ray/blob/master/.vale/styles/Google/WordList.yml).

## Troubleshooting

If you run into a problem building the docs, following these steps can help isolate or eliminate most issues:

1. **Clean out build artifacts.** Use `make clean` to clean out docs build artifacts in the working directory. Sphinx uses caching to avoid doing work, and this sometimes causes problems. This is particularly true if you build the docs, then `git pull origin master` to pull in recent changes, and then try to build docs again.
2. **Check your environment.** Use `pip list` to check the installed dependencies. Compare them to `doc/requirements-doc.txt`. The documentation build system doesn't have the same dependency requirements as Ray. You don't need to run ML models or execute code on distributed systems in order to build the docs. In fact, it's best to use a completely separate docs build environment from the environment you use to run Ray to avoid dependency conflicts.  When installing requirements, do `pip install -r doc/requirements-doc.txt`. Don't use `-U` because you don't want to upgrade any dependencies during the installation. To check your environment against Read the Docs automatically, run `make rtd-doctor`, which compares your interpreter and the pinned docs dependencies to the versions Read the Docs uses and tells you what to fix.
3. **Match the Read the Docs Python version.** The docs build system doesn't keep the same dependency and Python version requirements as Ray. Read the Docs builds with the Python version pinned in `.readthedocs.yaml` (currently 3.11), so use that same version locally; building with a different version can surface or hide warnings that then behave differently on Read the Docs.
4. **Enable breakpoints in Sphinx.** Add `-P` to the `SPHINXOPTS` in `doc/Makefile` to tell `sphinx` to stop when it encounters a breakpoint, and remove `-j auto` to disable parallel builds. Now you can put breakpoints in the modules you're trying to import, or in `sphinx` code itself, which can help isolate stubborn build issues.
5. **[Incremental build] Side navigation bar doesn't reflect new pages.** If you're adding new pages, they should always show up in the side navigation bar on index pages. However, incremental builds with `make local` skip rebuilding many other pages, so Sphinx doesn't update the side navigation bar on those pages. To build docs with a correct side navigation bar on all pages, consider using `make develop`.

## Where to go from here?

There are many ways to contribute to Ray other than documentation. See {doc}`our contributor guide <getting-involved>` for more information.
