Frequently asked questions
--------------------------

Here we try to answer questions that come up often. If you still have questions
after reading this, let us know!

.. contents::
    :local:
    :depth: 1

Which search algorithm/scheduler should I choose?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Tune offers :ref:`many different search algorithms <tune-search-alg>`
and :ref:`schedulers <tune-schedulers>`.
Deciding on which to use mostly depends on your problem:

* Is it a small or large problem (how long does it take to train? How costly
  are the resources, like GPUs)? Can you run many trials in parallel?
* How many hyperparameters would you like to tune?
* What values are valid for hyperparameters?

**If your model is small**, you can usually try to run many different configurations.
A **random search** can be used to generate configurations. You can also grid search
over some values. You should probably still use
:ref:`ASHA for early termination of bad trials <tune-scheduler-hyperband>`.

**If your model is large**, you can try to either use
**Bayesian Optimization-based search algorithms** like :ref:`BayesOpt <bayesopt>` or
:ref:`Dragonfly <Dragonfly>` to get good parameter configurations after few
trials. :ref:`Ax <tune-ax>` is similar but more robust to noisy data.
Please note that these algorithms only work well with **a small number of hyperparameters**.
Alternatively, you can use :ref:`Population Based Training <tune-scheduler-pbt>` which
works well with few trials, e.g. 8 or even 4. However, this will output a hyperparameter *schedule* rather
than one fixed set of hyperparameters.

**If you have a small number of hyperparameters**, Bayesian Optimization-methods
work well. Take a look at :ref:`BOHB <tune-scheduler-bohb>` to combine the
benefits of bayesian optimization with early stopping.

**If you only have continuous values for hyperparameters** this will work well
with most Bayesian-Optimization methods. Discrete or categorical variables still
work, but less good with an increasing number of categories.

**Our go-to solution** is usually to use **random search** with :ref:`ASHA for early stopping <tune-scheduler-hyperband>`
for smaller problems. Use :ref:`BOHB <tune-scheduler-bohb>` for **larger problems** with a **small number of hyperparameters**
and :ref:`Population Based Training <tune-scheduler-pbt>` for **larger problems** with a **large number of hyperparameters**
if a learning schedule is acceptable.

How do I choose hyperparameter ranges?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A good start is to look at the papers that introduced the algorithms, and also
to see what other people are using.

Most algorithms also have sensible defaults for some of their parameters.
For instance, `XGBoost's parameter overview <https://xgboost.readthedocs.io/en/latest/parameter.html>`_
reports to use ``max_depth=6`` for the maximum decision tree depth. Here, anything
between 2 and 10 might make sense (though that naturally depends on your problem).

For **learning rates**, we suggest using a **loguniform distribution** between
**1e-5** and **1e-1**: ``tune.loguniform(1e-5, 1e-1)``.

For **batch sizes**, we suggest trying **powers of 2**, for instance, 2, 4, 8,
16, 32, 64, 128, 256, etc. The magnitude depends on your problem. For easy
problems with lots of data, use higher batch sizes, for harder problems with
not so much data, use lower batch sizes.

For **layer sizes** we also suggest trying **powers of 2**. For small problems
(e.g. Cartpole), use smaller layer sizes. For larger problems, try larger ones.

For **discount factors** in reinforcement learning we suggest sampling uniformly
between 0.9 and 1.0. Depending on the problem, a much stricter range above 0.97
or oeven above 0.99 can make sense (e.g. for Atari).

How can I use nested/conditional search spaces?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sometimes you might need to define parameters whose value depend on the value
of other parameters. Ray Tune offers some methods to define these.

Nested spaces
'''''''''''''
You can nest hyperparameter definition in sub dictionaries:

.. code-block:: python

    config = {
        "a": {
            "x": tune.uniform(0, 10)
        },
        "b": tune.choice([1, 2, 3])
    }

The trial config will be nested exactly like the input config.

Conditional spaces
''''''''''''''''''
:ref:`Custom and conditional search spaces are explained in detail here <tune_custom-search>`.
In short, you can pass custom functions to ``tune.sample_from()`` that can
return values that depend on other values:

.. code-block:: python

    config = {
        "a": tune.randint(5, 10)
        "b": tune.sample_from(lambda spec: np.random.randint(0, spec.config.a))
    }

Conditional grid search
'''''''''''''''''''''''
If you would like to grid search over two parameters that depend on each other,
this might not work out of the box. For instance say that *a* should be a value
between 5 and 10 and *b* should be a value between 0 and a. In this case, we
cannot use ``tune.sample_from`` because it doesn't support grid searching.

The solution here is to create a list of valid *tuples* with the help of a
helper function, like this:

.. code-block:: python

    def _iter():
        for a in range(5, 10):
            for b in range(a):
                yield a, b

    config = {
        "ab": tune.grid_search(list(_iter())),
    }

Your trainable then can do something like ``a, b = config["ab"]`` to split
the a and b variables and use them afterwards.

How does early termination (e.g. Hyperband/ASHA) work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Early termination algorithms look at the intermediately reported values,
e.g. what is reported to them via ``tune.report()`` after each training
epoch. After a certain number of steps, they then remove the worst
performing trials and keep only the best performing trials. Goodness of a trial
is determined by ordering them by the objective metric, for instance accuracy
or loss.

In ASHA, you can decide how many trials are early terminated.
``reduction_factor=4`` means that only 25% of all trials are kept each
time they are reduced. With ``grace_period=n`` you can force ASHA to
train each trial at least for ``n`` epochs.

Why are all my trials returning "1" iteration?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**This is most likely applicable for the Tune function API.**

Ray Tune counts iterations internally every time ``tune.report()`` is
called. If you only call ``tune.report()`` once at the end of the training,
the counter has only been incremented once. If you're using the class API,
the counter is increased after calling ``step()``.

Note that it might make sense to report metrics more often than once. For
instance, if you train your algorithm for 1000 timesteps, consider reporting
intermediate performance values every 100 steps. That way, schedulers
like Hyperband/ASHA can terminate bad performing trials early.

What are all these extra outputs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You'll notice that Ray Tune not only reports hyperparameters (from the
``config``) or metrics (passed to ``tune.report()``), but also some other
outputs.

.. code-block:: bash

    Result for easy_objective_c64c9112:
      date: 2020-10-07_13-29-18
      done: false
      experiment_id: 6edc31257b564bf8985afeec1df618ee
      experiment_tag: 7_activation=tanh,height=-53.116,steps=100,width=13.885
      hostname: ubuntu
      iterations: 0
      iterations_since_restore: 1
      mean_loss: 4.688385317424468
      neg_mean_loss: -4.688385317424468
      node_ip: 192.168.1.115
      pid: 5973
      time_since_restore: 7.605552673339844e-05
      time_this_iter_s: 7.605552673339844e-05
      time_total_s: 7.605552673339844e-05
      timestamp: 1602102558
      timesteps_since_restore: 0
      training_iteration: 1
      trial_id: c64c9112

See the :ref:`tune-autofilled-metrics` section for a glossary.

How do I set resources?
~~~~~~~~~~~~~~~~~~~~~~~
If you want to allocate specific resources to a trial, you can use the
``resources_per_trial`` parameter of ``tune.run()``:

.. code-block:: python

    tune.run(
        train_fn,
        resources_per_trial={
            "cpu": 2,
            "gpu": 0.5,
            "extra_cpu": 2,
            "extra_gpu": 0
        })

The example above showcases three things:

1. The `cpu` and `gpu` options set how many CPUs and GPUs are available for
   each trial, respectively. **Trials cannot request more resources** than these
   (exception: see 3).
2. It is possible to request **fractional GPUs**. A value of 0.5 means that
   half of the memory of the GPU is made available to the trial. You will have
   to make sure yourself that your model still fits on the fractional memory.
3. You can **request extra resources** that are reserved for the trial. This
   is useful if your trainable starts another process that requires resources.
   This is for instance the case in some distributed computing settings,
   including when using RaySGD.

One important thing to keep in mind is that each Ray worker (and thus each
Ray Tune Trial) will only be scheduled on **one machine**. That means if
you for instance request 2 GPUs for your trial, but your cluster consists
of 4 machines with 1 GPU each, the trial will never be scheduled.

In other words, you will have to make sure that your Ray cluster
has machines that can actually fulfill your resource requests.

How can I pass further parameter values to my trainable function?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**This is only applicable for the Tune function API.**

Ray Tune expects your trainable functions to accept only up to two parameters,
``config`` and ``checkpoint_dir``. But sometimes there are cases where
you want to pass constant arguments, like the number of epochs to run,
or a dataset to train on. Ray Tune offers a wrapper function to achieve
just that, called ``tune.with_parameters()``:

.. code-block:: python

    from ray import tune

    import numpy as np

    def train(config, checkpoint_dir=None, num_epochs=10, data=None):
        for i in range(num_epochs):
            for sample in data:
                # ... train on sample

    # Some huge dataset
    data = np.random.random(size=100000000)

    tune.run(
        tune.with_parameters(train, num_epochs=10, data=data))


This function works similarly to ``functools.partial``, but it stores
the parameters directly in the Ray object store. This means that you
can pass even huge objects like datasets, and Ray makes sure that these
are efficiently stored and retrieved on your cluster machines.

How can I reproduce experiments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reproducing experiments and experiment results means that you get the exact same
results when running an experiment again and again. To achieve this, the
conditions have to be exactly the same each time you run the exeriment.
In terms of ML training and tuning, this mostly concerns
the random number generators that are used for sampling in various places of the
training and tuning lifecycle.

Random number generators are used to create randomness, for instance to sample a hyperparameter
value for a parameter you defined. There is no true randomness in computing, rather
there are sophisticated algorithms that generate numbers that *seem* to be random and
fulfill all properties of a random distribution. These algorithms can be *seeded* with
an initial state, after which the generated random numbers are always the same.

.. code-block:: python

    import random
    random.seed(1234)
    print([random.randint(0, 100) for _ in range(10)])

    # The output of this will always be
    # [99, 56, 14, 0, 11, 74, 4, 85, 88, 10]


The most commonly used random number generators from Python libraries are those in the
native ``random`` submodule and the ``numpy.random`` module.

.. code-block:: python

    # This should suffice to initialize the RNGs for most Python-based libraries
    import random
    import numpy as np
    random.seed(1234)
    np.random.seed(5678)

In your tuning and training run, there are several places where randomness occurrs, and
at all these places we will have to introduce seeds to make sure we get the same behavior.

* **Search algorithm**: Search algorithms have to be seeded to generate the same
  hyperparameter configurations in each run. Some search algorithms can be explicitly instantiated with a
  random seed (look for a ``seed`` parameter in the constructor). For others, try to use
  the above code block.
* **Schedulers**: Schedulers like Population Based Training rely on resampling some
  of the parameters, requiring randomness. Use the code block above to set the initial
  seeds.
* **Training function**: In addition to initializing the configurations, the training
  functions themselves have to use seeds. This could concern e.g. the data splitting.
  You should make sure to set the seed at the start of your training function.

PyTorch and TensorFlow use their own RNGs, which have to be initialized, too:

.. code-block:: python

    import torch
    torch.manual_seed(0)

    import tensorflow as tf
    tf.random.set_seed(0)

You should thus seed both Ray Tune's schedulers and search algorithms, and the
training code. The schedulers and search algorithms should always be seeded with the
same seed. This is also true for the training code, but often it is beneficial that
the seeds differ *between different training runs*.

Here's a blueprint on how to do all this in your training code:

.. code-block:: python

    import random
    import numpy as np
    from ray import tune


    def trainable(config):
        # config["seed"] is set deterministically, but differs between training runs
        random.seed(config["seed"])
        np.random.seed(config["seed"])
        # torch.manual_seed(config["seed"])
        # ... training code


    config = {
        "seed": tune.randint(0, 10000),
        # ...
    }

    if __name__ == "__main__":
        # Set seed for the search algorithms/schedulers
        random.seed(1234)
        np.random.seed(1234)
        # Don't forget to check if the search alg has a `seed` parameter
        tune.run(
            trainable,
            config=config
        )

**Please note** that it is not always possible to control all sources of non-determinism.
For instance, if you use schedulers like ASHA or PBT, some trials might finish earlier
than other trials, affecting the behavior of the schedulers. Which trials finish first
can however depend on the current system load, network communication, or other factors
in the envrionment that we cannot control with random seeds. This is also true for search
algorithms such as Bayesian Optimization, which take previous results into account when
sampling new configurations. This can be tackled by
using the **synchronous modes** of PBT and Hyperband, where the schedulers wait for all trials to
finish an epoch before deciding which trials to promote.

We strongly advise to try reproduction on smaller toy problems first before relying
on it for larger experiments.




Further Questions or Issues?
----------------------------

.. include:: /_help.rst
