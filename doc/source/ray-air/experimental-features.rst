.. _air-experimental-features:

================================
Experimental features in Ray AIR
================================

We're testing a number of experimental features in Ray AIR.

During development and internal dogfooding, the features
are disabled per default. They can be opted in by setting
an environment variable.

After some time, we enable the feature per default to gather
more feedback from the community. In that case, they can still
be disabled using the same environment variable. This will
fully revert to the old behavior, should the new behavior
not suffice your needs.

In that case, please `open an issue <https://github.com/ray-project/ray/issues/>`_
on GitHub and let us know what you're missing! This will give
us the chance to incorporate your feedback before we remove
the old implementation and make the new implementation the
default.

.. note::

    Experimental features can undergo frequent changes,
    especially on the master branch and the nightly wheels.

.. _air-experimental-new-output:

New output engine
-----------------

We've added a new output engine for Ray Train and Ray Tune runs.

The new output engine will affect how the training progress
is printed in the console.

The new features include:

- Ray Train runs will report status relevant to the single training run.
  It will not use the default Ray Tune table layout from before.
- The table format has been updated.
- We report configurations and observed metrics in a different format
- The default metrics displayed for e.g. RLlib runs are heavily reduce
  in the console output.
- We generally uncluttered a lot of output and made it easier to
  read.

The new output currently only works for the regular console.
Notably, it is automatically disabled when Jupyter Notebooks
or Ray client is used.


.. note::

    This feature is *enabled per default* starting Ray 2.5.

    To disable, set the environment variable ``RAY_AIR_NEW_OUTPUT=0``.


.. _air-experimental-rich:

Sticky table layout
-------------------

As part of the :ref:`new output engine <air-experimental-new-output>`,
we've introduced a sticky table layout. The regular console
logs will continue to be printed as before, but the trial
overview table (in Ray Tune) will be stuck to the bottom of the
screen and periodically updated.

This feature is still in development. You can opt-in to try
it out.

.. note::

    This feature is *disabled per default*.

    To enable, set the environment variable ``RAY_AIR_ENABLE_RICH=1``.


.. _air-experimental-execution:

New trial execution engine
--------------------------

We've implemented a new trial execution engine in Ray Tune.
Since Ray Tune is currently also the execution backend for
Ray Train, this affects both tuning and training runs.

Technically, we've refactored the :ref:`TrialRunner <trialrunner-docstring>`
to use a new, generic Ray actor and future manager instead of
the old ``RayTrialExecutor``.

Our CI and release tests all run with the new execution engine.
In most cases, you should not see any change to the previous
behavior.

However, if you notice any odd behavior, you can try to opt out of
the new execution engine and see if it resolves your problem.

In that case, please `open an issue <https://github.com/ray-project/ray/issues/>`_
on GitHub, ideally with a reproducible script.

Things to look out for:

- Less trials are running in parallel than before
- It takes longer to start new trials (or goes much faster)
- The tuning run finished, but the script does not exit
- The end-to-end runtime is much slower than before
- The CPU load on the head node is high,
  even though the training jobs don't
  require many resources or don't run on the head node
- Any exceptions are raised that indicate an error in starting or
  stopping trials or the experiment.

Again, we're testing against all these potential regressions, and
mitigated every bug we found. But there are sometimes edge cases
we don't capture, yet. Your feedback is very welcome here.

.. note::

    This feature is *enabled per default* starting Ray 2.5.

    To disable, set the environment variable ``TUNE_NEW_EXECUTION=0``.
