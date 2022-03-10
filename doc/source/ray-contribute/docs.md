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

There are many ways to contribute to the Ray documentation, and we're always looking for new contributors.
Even if you just want to fix a typo or expand on a section, please feel free to do so!

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

Before committing any changes, make sure you run `../scripts/format.sh` from the `doc` folder,
to make sure your changes are formatted correctly.

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

If you take Ray Tune as an example, you can see that our documentation is made up from several types of documentation
that you can all contribute to:

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
  choice. Most of the documents in Ray Tune are written in this way, for instance the
  [key concepts](https://github.com/ray-project/ray/blob/master/doc/source/tune/key-concepts.rst) or
  [API documentation](https://github.com/ray-project/ray/blob/master/doc/source/tune/api_docs/overview.rst).
- Notebooks, written in `.ipynb` format. All Tune examples are written as notebooks. These notebooks render in
  the browser like `.md` or `.rst` files, but have the added benefit of adding launch buttons to the top of the
  document, so that users can run the code themselves in either Binder or Google Colab. A good first example to look
  at is [this Tune example](https://github.com/ray-project/ray/blob/master/doc/source/tune/examples/tune-serve-integration-mnist.ipynb)

## Fixing typos and improving explanations

If you spot a typo in any document, or think that an explanation is not clear enough, please consider
opening a pull request.
In this scenario you don't need to add any new tests.
Just run the linter and submit your pull request.

## Adding API references

We use [Sphinx's autodoc extension](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html) to generate
our API documentation from our source code.
In case we're missing a reference to a function or class, please consider adding it to the respective document in question.

For example, here's how you can add a function or class reference using `autofunction` and `autoclass`:

```markdown
.. autofunction:: ray.tune.integration.docker.DockerSyncer

.. autoclass:: ray.tune.integration.keras.TuneReportCallback
```

The above snippet was taken from the
[Tune API documentation](https://github.com/ray-project/ray/blob/master/doc/source/tune/api_docs/integration.rst),
which you can look at for reference.

If you want to change the content of the API documentation, you will have to edit the respective function or class
signatures directly in the source code.
For example, in the above `autofunction` call, to change the API reference for `ray.tune.integration.docker.DockerSyncer`,
you would have to [change the following source file](https://github.com/ray-project/ray/blob/7f1bacc7dc9caf6d0ec042e39499bbf1d9a7d065/python/ray/tune/integration/docker.py#L15-L38).

## Adding code to an `.rST` or `.md` file

Modifying text in an existing documentation file is easy, but you need to be careful when it comes to adding code.
The reason is that we want to ensure every code snippet on our documentation is tested.
This requires us to have a process for including and testing code snippets in documents.

In an `.rST` or `.md` file, you can add code snippets using `literalinclude` from the Sphinx system.
For instance, here's an example from the Tune's "Key Concepts" documentation: 

```markdown
.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __function_api_start__
    :end-before: __function_api_end__
```

Note that in the whole file there's not a single literal code block, code _has to be_ imported using the `literalinclude` directive.
The code that gets added to the document by `literalinclude`, including `start-after` and `end-before` tags,
reads as follows:

```python
# __function_api_start__
from ray import tune


def objective(x, a, b):  # Define an objective function.
    return a * (x ** 0.5) + b


def trainable(config):  # Pass a "config" dictionary into your trainable.

    for x in range(20):  # "Train" for 20 iterations and compute intermediate scores.
        score = objective(x, config["a"], config["b"])

        tune.report(score=score)  # Send the score to Tune.


# __function_api_end__
```

This code is imported by `literalinclude` from a file called `doc_code/key_concepts.py`.
Every Python file in the `doc_code` directory will automatically get tested by our CI system,
but make sure to run scripts that you change (or new scripts) locally first.

In rare situations, when you're adding _obvious_ pseudo-code to demonstrate a concept, it is ok to add it
literally into your `.rST` or `.md` file, e.g. using a `.. code-block:: python` directive.
But if your code is supposed to run, it needs to be tested.

## Creating a new document from scratch

Sometimes you might want to add a completely new document to the Ray documentation, like adding a new
user guide or a new example.

For this to work, you need to make sure to add the new document explicitly to the 
[`_toc.yml` file](https://github.com/ray-project/ray/blob/master/doc/source/_toc.yml) that determines
the structure of the Ray documentation.

Depending on the type of document you're adding, you might also have to make changes to an existing overview
page that curates the list of documents in question.
For instance, for Ray Tune each user guide is added to the
[user guide overview page](https://docs.ray.io/en/master/tune/tutorials/overview.html) as a panel, and the same
goes for [all Tune examples](https://docs.ray.io/en/master/tune/examples/index.html).
Always check the structure of the Ray sub-project whose documentation you're working on to see how to integrate
it within the existing structure.

## Creating a notebook example

To add a new executable example to the Ray documentation, you can start from our
[MyST notebook template](https://github.com/ray-project/ray/tree/master/doc/source/_templates/template.md) or
[Jupyter notebook template](https://github.com/ray-project/ray/tree/master/doc/source/_templates/template.ipynb).
You could also simply download the document you're reading right now (click on the respective download button at the
top of this page to get the `.ipynb` file) and start modifying it.
All the example notebooks in Ray Tune get automatically tested by our CI system, provided you place them in the
[`examples` folder](https://github.com/ray-project/ray/tree/master/doc/source/tune/examples).
If you have questions about how to test your notebook when contributing to other Ray sub-projects, please make
sure to ask a question in [the Ray community Slack](https://forms.gle/9TSdDYUgxYs8SA9e8) or directly on GitHub,
when opening your pull request.

To work off of an existing example, you could also have a look at the
[Ray Tune Hyperopt example (`.ipynb`)](https://github.com/ray-project/ray/blob/master/doc/source/tune/examples/hyperopt_example.ipynb)
or the [Ray Serve guide for RLlib (`.md`)](https://github.com/ray-project/ray/blob/master/doc/source/serve/tutorials/rllib.md).
We recommend that you start with an `.md` file and convert your file to an `.ipynb` notebook at the end of the process.
We'll walk you through this process below.

What makes these notebooks different from other documents is that they combine code and text in one document,
and can be launched in the browser.
We also make sure they are tested by our CI system, before we add them to our documentation.
To make this work, notebooks need to define a _kernel specification_ to tell a notebook server how to interpret
and run the code.
For instance, here's the kernel specification of a Python notebook:

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

If you write a notebook in `.md` format, you need this YAML front matter at the top of the file.
To add code to your notebook, you can use the `code-block` directive.
Here's an example:

````markdown
```{code-cell} python3
:tags: [hide-cell]

import ray
import ray.rllib.agents.ppo as ppo
from ray import serve

def train_ppo_model():
    trainer = ppo.PPOTrainer(
        config={"framework": "torch", "num_workers": 0},
        env="CartPole-v0",
    )
    # Train for one iteration
    trainer.train()
    trainer.save("/tmp/rllib_checkpoint")
    return "/tmp/rllib_checkpoint/checkpoint_000001/checkpoint-1"


checkpoint_path = train_ppo_model()
```
````

Putting this markdown block into your document will render as follows in the browser:

```{code-cell} python3
:tags: [hide-cell]

import ray
import ray.rllib.agents.ppo as ppo
from ray import serve

def train_ppo_model():
    trainer = ppo.PPOTrainer(
        config={"framework": "torch", "num_workers": 0},
        env="CartPole-v0",
    )
    # Train for one iteration
    trainer.train()
    trainer.save("/tmp/rllib_checkpoint")
    return "/tmp/rllib_checkpoint/checkpoint_000001/checkpoint-1"


checkpoint_path = train_ppo_model()
```

As you can see, the code block is hidden, but you can expand it by click on the "+" button.

### Tags for your notebook

What makes this work is the `:tags: [hide-cell]` directive. in the `code-block`.
The reason we suggest starting with `.md` files is that it's much easier to add tags to them, as you've just seen.
You can also add tags to `.ipynb` files, but you'll need to start a notebook server for that first, which may
not want to do to contribute a piece of documentation.

Apart from `hide-cell`, you also have `hide-input` and `hide-output` tags that hide the input and output of a cell.
Also, if you need code that gets executed in the notebook, but you don't want to show it in the documentation,
you can use the `remove-cell`, `remove-input`, and `remove-output` tags in the same way.

### Testing notebooks

Removing cells can be particularly interesting for compute-intensive notebooks.
We want you to contribute notebooks that use _realistic_ values, not just toy examples.
At the same time we want our notebooks to be tested by our CI system, and running them should not take too long.
What you can do to address this is to have notebook cells with the parameters you want the users to see first:

````markdown
```{code-cell} python3
num_workers = 8
num_gpus = 2
```
````

which will render as follows in the browser:
 
```{code-cell} python3
num_workers = 8
num_gpus = 2
```

But then in your notebook you follow that up with a _removed_ cell that won't get rendered, but has much smaller
values and make the notebook run faster:

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

In the same way, you can convert `.ipynb` notebooks to `.md` notebooks with `--to myst`.
And if you want to convert your notebook to a Python file, e.g. to test if your whole script runs without errors,
you can use `--to py` instead.

## Where to go from here?

There are many other ways to contribute to Ray other than documentation.
See {ref}`our contributor guide <getting-involved>` for more information.
