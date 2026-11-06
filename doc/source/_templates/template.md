---
myst:
  html_meta:
    description: "<One or two sentences that say what the reader builds and which Ray libraries the guide uses.>"
---

<!--
Template for an end-to-end guide in the Ray documentation.

Copy this file into the directory of the library your guide covers, rename it, and replace every <placeholder>. Then add the page to a toctree. For Ray Data, Ray Serve, and Ray Train, you can list the page in the library's examples.yml gallery instead. Delete this comment before you open a pull request, but keep the maintainer comment under the title.

Ray CI doesn't run the code in this guide. Run every step yourself, record the Ray version you used in the maintainer comment below the title, and update that version whenever you re-verify the guide.

Write the guide as plain MyST Markdown. Don't add Jupyter front matter such as `jupytext` or `kernelspec`, `{code-cell}` blocks, or a companion `.ipynb` file. The docs build doesn't execute them, and the Ray documentation is retiring in-tree notebooks. To publish an example that Ray tests and keeps current, use the template-collections flow in doc/source/ray-contribute/publishing-examples.md instead.

Follow the Ray documentation style guide in doc/source/ray-contribute/writing-style.md.
-->

(<your-guide-label>)=

# <Task-oriented title in sentence case, such as "Classify images in parallel with Ray Data">

This guide shows how to <do the task>. <One or two sentences on what the reader has at the end and why they'd want it.>

<!-- Maintainers: Ray CI doesn't test this guide. Its author last ran every step with Ray <x.y.z>. -->

## Prerequisites

Before you start, make sure you have the following:

- Python <version> or later.
- <Hardware, accounts, credentials, or datasets the guide needs, such as a GPU or an S3 bucket.>

## Install dependencies

Install Ray and the other packages this guide uses:

```bash
pip install -U "ray[default]" <other-packages>
```

## Start Ray

Start Ray on your machine, or connect to a Ray cluster that's already running:

::::{tab-set}

:::{tab-item} Local machine
```python
import ray

# Start a local Ray instance.
ray.init()
```
:::

:::{tab-item} Existing cluster
```python
import ray

# Connect to the cluster that `ray start` started on this machine.
ray.init(address="auto")
```
:::

::::

To learn more about starting Ray, see {ref}`start-ray-cli`.

(<your-guide-label>-run)=

## <First step, written as an imperative, such as "Define the work">

<Explain what this step does and why, in prose. Introduce each code block with a sentence.>

The following function squares a number. The {py:func}`ray.remote` decorator turns it into a task that Ray can schedule on any worker in the cluster:

```python
@ray.remote
def square(x: int) -> int:
    return x * x
```

## <Next step, such as "Run the tasks in parallel">

Submit four tasks and wait for the results with {py:func}`ray.get`:

```python
# Submit the tasks. Each call returns immediately with an object reference.
futures = [square.remote(i) for i in range(4)]

# Block until every task finishes, then collect the results.
print(ray.get(futures))
```

The script prints the following:

```text
[0, 1, 4, 9]
```

<!--
Show output that's long or that the reader only sometimes needs in a dropdown, so it doesn't interrupt the steps.
-->

:::{dropdown} Example: Full output
```text
<Paste the complete output from your own run here.>
```
:::

:::{tip}
<An optional shortcut or a suggestion for scaling the example up.>
:::

<!--
To add a diagram or a screenshot, put the image in an images/ directory next to your guide, link it with a relative path, and give it alt text that says what it shows:

![<What the image shows>](images/<your-image>.png)
-->

## Clean up

Disconnect from Ray when you're done:

```python
ray.shutdown()
```

If you started a local Ray instance, this also stops it.

## Next steps

To keep learning, see the following pages:

- {ref}`core-walkthrough`
- {ref}`cluster-index`
- <Links to the library guides and API pages that go deeper on what this guide covers.>
