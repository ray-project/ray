---
myst:
  html_meta:
    description: "Guide to writing runnable, CI-tested code examples for Ray docs using doctest-style, code-output-style, and literalinclude formats, and to debugging one that fails. Read this when adding code snippets to docstrings or user guides so they keep working for users."
---

(writing-code-snippets_ref)=

# How to write code snippets

Users learn from example. So, whether you're writing a docstring or a user guide, include examples that illustrate the relevant APIs. Your examples should run out-of-the-box so that users can copy them and adapt them to their own needs.

This page describes how to write code snippets so that they're tested in CI.

:::{note}
The examples in this guide use reStructuredText. If you're writing Markdown, use MyST syntax. To learn more, read the [MyST documentation](https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html#directives-a-block-level-extension-point).
:::

## Types of examples

There are three types of examples: *doctest-style*, *code-output-style*, and *literalinclude*.

### *doctest-style* examples

*doctest-style* examples mimic interactive Python sessions.

```
.. doctest::

    >>> def is_even(x):
    ...     return (x % 2) == 0
    >>> is_even(0)
    True
    >>> is_even(1)
    False
```

They're rendered like this:

```{doctest}
>>> def is_even(x):
...     return (x % 2) == 0
>>> is_even(0)
True
>>> is_even(1)
False
```

:::{tip}
If you're writing docstrings, exclude `.. doctest::` to simplify your code:

```
Example:
    >>> def is_even(x):
    ...     return (x % 2) == 0
    >>> is_even(0)
    True
    >>> is_even(1)
    False
```
:::

### *code-output-style* examples

*code-output-style* examples contain ordinary Python code.

```
.. testcode::

    def is_even(x):
        return (x % 2) == 0

    print(is_even(0))
    print(is_even(1))

.. testoutput::

    True
    False
```

They're rendered like this:

```{testcode}
def is_even(x):
    return (x % 2) == 0

print(is_even(0))
print(is_even(1))
```

```{testoutput}
True
False
```

### *literalinclude* examples

*literalinclude* examples display Python modules.

```
.. literalinclude:: ./doc_code/example_module.py
    :language: python
    :start-after: __is_even_begin__
    :end-before: __is_even_end__
```

```{literalinclude} ./doc_code/example_module.py
:language: python
```

They're rendered like this:

```{literalinclude} ./doc_code/example_module.py
:language: python
:start-after: __is_even_begin__
:end-before: __is_even_end__
```

## Which type of example should you write?

There's no hard rule about which style you should use. Choose the style that best illustrates your API.

:::{tip}
If you're not sure which style to use, use *code-output-style*.
:::

### When to use *doctest-style*

If you're writing a small example that emphasizes object representations, or if you want to print intermediate objects, use *doctest-style*.

```
.. doctest::

    >>> import ray
    >>> ds = ray.data.range(100)
    >>> ds.schema()
    Column  Type
    ------  ----
    id      int64
    >>> ds.take(5)
    [{'id': 0}, {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]
```

### When to use *code-output-style*

If you're writing a longer example, or if object representations aren't relevant to your example, use *code-output-style*.

```
.. testcode::

    from typing import Dict
    import numpy as np
    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    # Compute a "petal area" attribute.
    def transform_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        vec_a = batch["petal length (cm)"]
        vec_b = batch["petal width (cm)"]
        batch["petal area (cm^2)"] = np.round(vec_a * vec_b, 2)
        return batch

    transformed_ds = ds.map_batches(transform_batch)
    print(transformed_ds.materialize())

.. testoutput::

    shape: (150, 6)
    ╭───────────────────┬──────────────────┬───────────────────┬──────────────────┬────────┬───────────────────╮
    │ sepal length (cm) ┆ sepal width (cm) ┆ petal length (cm) ┆ petal width (cm) ┆ target ┆ petal area (cm^2) │
    │ ---               ┆ ---              ┆ ---               ┆ ---              ┆ ---    ┆ ---               │
    │ double            ┆ double           ┆ double            ┆ double           ┆ int64  ┆ double            │
    ╞═══════════════════╪══════════════════╪═══════════════════╪══════════════════╪════════╪═══════════════════╡
    │ 5.1               ┆ 3.5              ┆ 1.4               ┆ 0.2              ┆ 0      ┆ 0.28              │
    │ 4.9               ┆ 3.0              ┆ 1.4               ┆ 0.2              ┆ 0      ┆ 0.28              │
    │ 4.7               ┆ 3.2              ┆ 1.3               ┆ 0.2              ┆ 0      ┆ 0.26              │
    │ 4.6               ┆ 3.1              ┆ 1.5               ┆ 0.2              ┆ 0      ┆ 0.3               │
    │ 5.0               ┆ 3.6              ┆ 1.4               ┆ 0.2              ┆ 0      ┆ 0.28              │
    │ …                 ┆ …                ┆ …                 ┆ …                ┆ …      ┆ …                 │
    │ 6.7               ┆ 3.0              ┆ 5.2               ┆ 2.3              ┆ 2      ┆ 11.96             │
    │ 6.3               ┆ 2.5              ┆ 5.0               ┆ 1.9              ┆ 2      ┆ 9.5               │
    │ 6.5               ┆ 3.0              ┆ 5.2               ┆ 2.0              ┆ 2      ┆ 10.4              │
    │ 6.2               ┆ 3.4              ┆ 5.4               ┆ 2.3              ┆ 2      ┆ 12.42             │
    │ 5.9               ┆ 3.0              ┆ 5.1               ┆ 1.8              ┆ 2      ┆ 9.18              │
    ╰───────────────────┴──────────────────┴───────────────────┴──────────────────┴────────┴───────────────────╯
    (Showing 10 of 150 rows)
```

### When to use *literalinclude*

If you're writing an end-to-end example and your example doesn't contain outputs, use *literalinclude*.

## How to handle hard-to-test examples

### When is it okay to not test an example?

You don't need to test examples that depend on external systems such as Weights and Biases.

### Skipping *doctest-style* examples

To skip a *doctest-style* example, append `# doctest: +SKIP` to your Python code.

```
.. doctest::

    >>> import ray
    >>> ray.data.read_images("s3://private-bucket")  # doctest: +SKIP
```

### Skipping *code-output-style* examples

To skip a *code-output-style* example, add `:skipif: True` to the `testcode` block.

```
.. testcode::
    :skipif: True

    from ray.air.integrations.wandb import WandbLoggerCallback
    callback = WandbLoggerCallback(
        project="Optimization_Project",
        api_key_file=...,
        log_config=True
    )
```

## How to handle long or non-deterministic outputs

If your Python code is non-deterministic, or if your output is excessively long, you can skip all or part of the output.

### Ignoring *doctest-style* outputs

To ignore parts of a *doctest-style* output, replace problematic sections with ellipses.

```
>>> import ray
>>> ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
Dataset(num_rows=..., schema=...)
```

To ignore an output altogether, write a *code-output-style* snippet. Don't use `# doctest: +SKIP`.

### Ignoring *code-output-style* outputs

If parts of your output are long or non-deterministic, replace problematic sections with ellipses.

```
.. testcode::

    import ray
    ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
    print(ds)

.. testoutput::

    Dataset(num_rows=..., schema=...)
```

If your output is non-deterministic and you want to display a sample output, add `:options: +MOCK`.

```
.. testcode::

    import random
    print(random.random())

.. testoutput::
    :options: +MOCK

    0.969461416250246
```

If your output is hard to test and you don't want to display a sample output, exclude the `testoutput`.

```
.. testcode::

    print("This output is hidden and untested")
```

## How to test a Ray Serve deployment

An HTTP check such as `assert response.status_code == 200` proves the endpoint answered, but it doesn't prove the deployment reached a healthy state. A Ray Serve application can fail to reach `RUNNING`, or land in `DEPLOY_FAILED` or `UNHEALTHY`, in ways a single request after `serve.run` doesn't reliably catch. To test the deployment lifecycle itself, poll `serve.status()` until the application is `RUNNING`, and raise if it reaches a failure state or times out.

Run the app without blocking, then poll its status:

```python
import time

from ray import serve
from ray.serve.schema import ApplicationStatus

# serve.run blocks by default, so start the app without blocking, then poll.
serve.run(app, blocking=False)

timeout_seconds = 180
start_time = time.time()
status = ApplicationStatus.NOT_STARTED

while status != ApplicationStatus.RUNNING and time.time() - start_time < timeout_seconds:
    status = next(iter(serve.status().applications.values())).status
    if status in (ApplicationStatus.DEPLOY_FAILED, ApplicationStatus.UNHEALTHY):
        raise AssertionError(f"Deployment failed with status: {status}")
    time.sleep(1)

if status != ApplicationStatus.RUNNING:
    raise AssertionError(
        f"Deployment didn't reach RUNNING within {timeout_seconds}s. Last status: {status}"
    )

serve.shutdown()
```

This pattern catches a broken deployment that an HTTP check misses, so use it for examples whose point is that an application deploys and serves traffic. It also works for a Ray Serve LLM app, which `build_openai_app` returns as an ordinary Serve application.

To keep the snippet the reader copies readable while still testing the deployment, put only the user-facing example inside the `literalinclude` markers and keep the polling outside them. The `qwen_example.py` module under `doc/source/llm/doc_code/serve/qwen/` does this: it embeds the user-facing block through `literalinclude` and polls `serve.status()` after it.

## How to test examples with GPUs

To configure Bazel to run an example with GPUs, complete the following steps:

1. Open the corresponding `BUILD` file. If your example is in the `doc/` folder, open `doc/BUILD`. If your example is in the `python/` folder, open a file such as `python/ray/train/BUILD`.

2. Locate the `doctest` rule. It looks like this:

   ```
   doctest(
       files = glob(
           include=["source/**/*.rst"],
       ),
       size = "large",
       tags = ["team:none"]
   )
   ```

3. Add the file that contains your example to the list of excluded files.

   ```
   doctest(
       files = glob(
           include=["source/**/*.rst"],
           exclude=["source/data/requires-gpus.rst"]
       ),
       tags = ["team:none"]
   )
   ```

4. If it doesn't already exist, create a `doctest` rule with `gpu` set to `True`.

   ```
   doctest(
       files = [],
       tags = ["team:none"],
       gpu = True
   )
   ```

5. Add the file that contains your example to the GPU rule.

   ```
   doctest(
       files = ["source/data/requires-gpus.rst"]
       size = "large",
       tags = ["team:none"],
       gpu = True
   )
   ```

For a practical example, see `doc/BUILD` or `python/ray/train/BUILD`.

## How to locally test examples

To locally test examples, install the Ray fork of `pytest-sphinx`.

```bash
pip install git+https://github.com/ray-project/pytest-sphinx
```

Then, run pytest on a module, docstring, or user guide.

```bash
pytest --doctest-modules python/ray/data/read_api.py
pytest --doctest-modules python/ray/data/read_api.py::ray.data.read_api.range
pytest --doctest-modules doc/source/data/getting-started.rst
```

## How to debug a failing example

When a code snippet fails in CI, two questions decide what you need to do about it: what kind of failure is it, and what was the example protecting. To find which step failed, see [Per-library docs example tests](ci.md#per-library-docs-example-tests), which maps each documentation path to the step that runs its examples.

### What kind of failure is it?

- **The example ran, but its output didn't match.** This is an ordinary test failure. Reproduce it locally with `pytest --doctest-modules <file>` and compare the actual output against the expected `testoutput` or `>>>` block. Either the code changed or the expected output is wrong.
- **The build failed before your example ran.** An import error or a `conf.py` error can abort the build, and the rest of the log is unreliable after an abort. Fix that error first, then re-read the log.
- **A Sphinx warning failed the build.** The doc site host (Read the Docs) render gate treats warnings as errors, so a malformed directive or a broken cross-reference fails the build even when your code is correct. This is a markup problem, not a code failure. See [the Read the Docs render gate](ci.md#the-read-the-docs-render-gate).

### What was the example protecting?

A failing example is a signal, but the right response depends on what the example was there to catch. There are three cases:

- **A breaking-change detector.** The snippet is user-facing code that broke because the library's behavior changed. Treat the failure as a real signal: either don't land the change, or land it with breaking-change communication. Once the change is approved, do update the example to match the new behavior, so the page stays correct for the version that ships. What's wrong is updating it *instead of* communicating the break, because that hides the change from the users who copied the old example.
- **An example validator.** The example itself is wrong, such as a typo, a bad merge, or a stale import, and just needs to run. Fix the example. Don't mistake it for a code regression.
- **A drift indicator.** The example still runs, but the prose around it has gone stale relative to what the code now does. Update the narrative, not just the example.

The same example can fail different ways at different times. An `IndentationError` introduced by a bad merge is an example-validator failure, so fix the snippet. An `ImportError` from an upstream API change to that same example is a breaking-change signal, so fix the code or communicate the break. The failure text tells you which case you're in.

Let the intent guide whether to skip. Skipping with `# doctest: +SKIP` or `:skipif: True` is right for an example that depends on an external system, but it's the wrong response to a breaking-change detector. Skipping there hides a real break from users.

The same reasoning applies to the [`docs-go` label](ci.md#skipping-example-tests-with-the-docs-go-label), which skips the per-library example steps for a whole PR. It's a convenience for a prose change that doesn't touch the examples, not a way past a red example test.
