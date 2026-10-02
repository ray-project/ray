---
myst:
  html_meta:
    description: "Worked Ray Core example that estimates π by Monte Carlo sampling, combining parallel remote tasks with a progress-tracking actor."
---

(monte-carlo-pi)=

# Monte Carlo estimation of π

```{raw} html
<a id="try-anyscale-quickstart-monte_carlo_pi" target="_blank" href="https://console.anyscale.com/register/ha?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_campaign=monte_carlo_pi">
  <img src="../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
  <br/><br/>
</a>
```

This tutorial shows you how to estimate the value of π using a [Monte Carlo method](https://en.wikipedia.org/wiki/Monte_Carlo_method) that works by randomly sampling points within a 2x2 square. The proportion of the points that fall within the unit circle centered at the origin estimates the ratio of the area of the circle to the area of the square. Because the true ratio is π/4, multiplying the estimated ratio by 4 approximates the value of π. The more points you sample, the closer the approximation should be to the true value of π.

```{image} ../images/monte_carlo_pi.png
:alt: Scatter plot of 1000 random points in a 2x2 square around a unit circle. Points inside the circle are green and points outside are red, giving an estimate of pi of about 3.164.
```

This tutorial uses Ray {ref}`tasks <ray-remote-functions>` to distribute the work of sampling and Ray {ref}`actors <ray-remote-classes>` to track the progress of these distributed sampling tasks. The code can run on your laptop, and you can scale it to large {ref}`clusters <cluster-index>` to increase the accuracy of the estimate.

To get started, install Ray with `pip install -U ray`. See {ref}`Installing Ray <installation>` for more installation options.

## Starting Ray
First, import the modules this tutorial needs and start a local Ray cluster with {func}`ray.init() <ray.init>`:

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __starting_ray_start__
:end-before: __starting_ray_end__
```

## Defining the progress actor
Next, define a Ray actor that sampling tasks can call to update progress. A Ray actor is essentially a stateful service. Anyone with a handle to the actor can call its methods.

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __defining_actor_start__
:end-before: __defining_actor_end__
```

To define a Ray actor, decorate a normal Python class with {func}`ray.remote <ray.remote>`. The progress actor has a `report_progress()` method, which each sampling task calls to update its own progress, and a `get_progress()` method, which gets the overall progress.

## Defining the sampling task
After you define the actor, define a Ray task that takes up to `num_samples` samples and returns the number of samples inside the circle. Ray tasks are stateless functions. They execute asynchronously and run in parallel.

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __defining_task_start__
:end-before: __defining_task_end__
```

To convert a normal Python function into a Ray task, decorate the function with {func}`ray.remote <ray.remote>`. The sampling task takes a progress actor handle as input and reports progress to it. The preceding code shows an example of calling actor methods from tasks.

## Creating a progress actor
After you define the actor, create an instance of it.

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __creating_actor_start__
:end-before: __creating_actor_end__
```

To create an instance of the progress actor, call the `ActorClass.remote()` method with arguments to the constructor. This call creates and runs the actor on a remote worker process. The return value of `ActorClass.remote(...)` is an actor handle that you use to call the actor's methods.

## Executing sampling tasks
After you define the task, execute it asynchronously.

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __executing_task_start__
:end-before: __executing_task_end__
```

To execute the sampling task, call its `remote()` method with arguments to the function. This call immediately returns an `ObjectRef` as a future and then executes the function asynchronously on a remote worker process.

## Calling the progress actor
While the sampling tasks run, query the progress periodically by calling the actor's `get_progress()` method.

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __calling_actor_start__
:end-before: __calling_actor_end__
```

To call an actor method, use `actor_handle.method.remote()`. This invocation immediately returns an `ObjectRef` as a future and then executes the method asynchronously on the remote actor process. To fetch the returned value of an `ObjectRef`, call the blocking {func}`ray.get() <ray.get>`.

## Calculating π
Finally, get the number of samples inside the circle from the remote sampling tasks and calculate π.

```{literalinclude} ../doc_code/monte_carlo_pi.py
:language: python
:start-after: __calculating_pi_start__
:end-before: __calculating_pi_end__
```

As the preceding code shows, {func}`ray.get() <ray.get>` can also take a list of `ObjectRef` objects instead of a single `ObjectRef`, and return a list of results.

If you run this tutorial, you see output similar to the following:

```text
Progress: 0%
Progress: 15%
Progress: 28%
Progress: 40%
Progress: 50%
Progress: 60%
Progress: 70%
Progress: 80%
Progress: 90%
Progress: 100%
Estimated value of π is: 3.1412202
```
