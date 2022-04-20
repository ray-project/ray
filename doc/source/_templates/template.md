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


(document-tag-to-refer-to)=

# Creating an Example

This is an example template file for writing Jupyter Notebooks in markdown, using MyST.
For more information on MyST notebooks, see the 
[MyST-NB documentation](https://myst-nb.readthedocs.io/en/latest/index.html).
If you want to learn more about the MyST parser, see the
[MyST documentation](https://myst-parser.readthedocs.io/en/latest/).

MyST is common markdown compliant, so if you can use plain markdown here.
In case you need to execute restructured text (`rSt`) directives, you can use `{eval-rst}` to execute the code.
For instance, a here's a note written in rSt:

```{eval-rst}
.. note::

   A note written in reStructuredText.
```

```{margin}
You can create margins with this syntax for smaller notes that don't make it into the main
text.
```

You can also easily define footnotes.[^example]

[^example]: This is a footnote.

## Adding code cells


```{code-cell} python3

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


## Hiding and removing cells

You can hide cells, so that they will toggle when you click on the cell header.
You can use different `:tags:` like `hide-cell`, `hide-input`, or `hide-output` to hide cell content,
and you can use `remove-cell`, `remove-input`, or `remove-output` to remove the cell completely when rendered.
Those cells will still show up in the notebook itself, e.g. when you launch it in binder.

```{code-cell} python3
:tags: [hide-cell]
# This can be useful if you don't want to clutter the page with details.

import ray
import ray.rllib.agents.ppo as ppo
from ray import serve
```

:::{tip}
Here's a quick tip.
:::


:::{note}
And this is a note.
:::

The following cell will be removed and not render:

```{code-cell} python3
:tags: [remove-cell]
ray.shutdown()
```

## Equations

\begin{equation}
\frac {\partial u}{\partial x} + \frac{\partial v}{\partial y} = - \, \frac{\partial w}{\partial z}
\end{equation}

\begin{align*}
2x - 5y &=  8 \\
3x + 9y &=  -12
\end{align*}
