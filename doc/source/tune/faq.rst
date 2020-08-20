.. _tune-faq:

==========================
Frequently asked questions
==========================

Here we try to answer questions that come up often. If you still have questions
after reading this, let us know!

.. contents::
    :local:
    :depth: 1

Which search algorithm/scheduler should I choose?
-------------------------------------------------
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
trials. Please note that this only works well with **a small number of hyperparameters**.
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
--------------------------------------
A good start is to look at the papers that introduced the algorithms, and also
to see what other people are using.

Most algorithms also have sensible defaults for some of their parameters.
For instance, `XGBoost's parameter overview <https://xgboost.readthedocs.io/en/latest/parameter.html>`_
reports to use ``max_depth=6`` for the maximum decision tree depth. Here, anything
between 2 and 10 might make sense (though that naturally depends on your problem).

For **learning rates**, we suggest using a **loguniform distribution** between
**1e-1** and **1e-5**: ``tune.loguniform(1e-1, 1e05)``.

For **batch sizes**, we suggest trying **powers of 2**, for instance, 2, 4, 8,
16, 32, 64, 128, 256, etc. The magnitude depends on your problem. For easy
problems with lots of data, use higher batch sizes, for harder problems with
not so much data, use lower batch sizes.

For **layer sizes** we also suggest trying **powers of 2**. For small problems
(e.g. Cartpole), use smaller layer sizes. For larger problems, try larger ones.

For **discount factors** in reinforcement learning we suggest sampling uniformly
between 0.9 and 1.0. Depending on the problem, a much stricter range above 0.97
or oeven above 0.99 can make sense (e.g. for Atari).

How does early termination (e.g. Hyperband/ASHA) work?
------------------------------------------------------
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
----------------------------------------------
Ray Tune counts iterations internally every time ``tune.report()`` is
called. If you only call ``tune.report()`` once at the end of the training,
the counter has only been incremented once. If you're using the class API,
the counter is increased after calling ``_train()``.

Note that it might make sense to report metrics more often than once. For
instance, if you train your algorithm for 1000 timesteps, consider reporting
intermediate performance values every 100 steps. That way, schedulers
like Hyperband/ASHA can terminate bad performing trials early.

What are all these extra outputs?
---------------------------------
You'll notice that Ray Tune not only reports hyperparameters (from the
``config``) or metrics (passed to ``tune.report()``), but also some other
outputs. The ``Trial.last_result`` dictionary contains the following
additional outputs:

* ``config``: The hyperparameter configuration
* ``date``: String-formatted date and time when the result was processed
* ``done``: True if the trial has been finished, False otherwise
* ``episodes_total``: Total number of episodes (for RLLib trainables)
* ``experiment_id``: Unique experiment ID
* ``experiment_tag``: Unique experiment tag (includes parameter values)
* ``hostname``: Hostname of the worker
* ``iterations_since_restore``: The number of times ``tune.report()`` has been
  called after restoring the run from a checkpoint
* ``node_ip``: Host IP of the worker
* ``pid``: Process ID (PID) of the worker process
* ``time_since_restore``: Time in seconds since restoring from a checkpoint.
* ``time_this_iter_s``: Runtime of the current training iteration in seconds (i.e.
  one call to the trainable function or to ``_train()`` in the class API.
* ``time_total_s``: Total runtime in seconds.
* ``timestamp``: Timestamp when the result was processed
* ``timesteps_since_restore``: Number of timesteps since restoring from a checkpoint
* ``timesteps_total``: Total number of timesteps
* ``training_iteration``: The number of times ``tune.report()`` has been
  called
* ``trial_id``: Unique trial ID

How do I set resources?
-----------------------
Todo


Further Questions or Issues?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reach out to us if you have any questions or issues or feedback through the following channels:

1. `StackOverflow`_: For questions about how to use Ray.
2. `GitHub Issues`_: For bug reports and feature requests.

.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
