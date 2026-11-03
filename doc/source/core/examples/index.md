---
myst:
  html_meta:
    description: "Ray Core code examples from beginner (Monte Carlo pi) to intermediate (MapReduce, web crawler) to advanced (parameter server, pong)."
---

(ray-core-examples-tutorial)=

# Ray Core examples

```{toctree}
:hidden:
:glob:

*
```

<!-- Organize example .rst files in the same manner as the
   .py files in ray/python/ray/train/examples. -->

The following examples show how to use Ray Core for a variety of use cases.

## Beginner

```{list-table}
* - {doc}`A Gentle Introduction to Ray Core by Example <gentle_walkthrough>`
* - {doc}`Using Ray for Highly Parallelizable Tasks <highly_parallel>`
* - {doc}`monte-carlo-pi`
```


## Intermediate

```{list-table}
* - {doc}`map_reduce`
* - {doc}`web_crawler`
```


## Advanced

```{list-table}
* - {doc}`batch_prediction`
* - {doc}`plot_parameter_server`
* - {doc}`Simple Parallel Model Selection <plot_hyperparameter>`
* - {doc}`Learning to Play Pong <plot_pong_example>`
```
