.. _tune-reporter-doc:


Tune Console Output (Reporters)
===============================

By default, Tune reports experiment progress periodically to the command-line as follows.

.. code-block:: bash

    == Status ==
    Memory usage on this node: 11.4/16.0 GiB
    Using FIFO scheduling algorithm.
    Resources requested: 4/12 CPUs, 0/0 GPUs, 0.0/3.17 GiB heap, 0.0/1.07 GiB objects
    Result logdir: /Users/foo/ray_results/myexp
    Number of trials: 4 (4 RUNNING)
    +----------------------+----------+---------------------+-----------+--------+--------+--------+--------+------------------+-------+
    | Trial name           | status   | loc                 |    param1 | param2 | param3 |    acc |   loss |   total time (s) |  iter |
    |----------------------+----------+---------------------+-----------+--------+--------+--------+--------+------------------+-------|
    | MyTrainable_a826033a | RUNNING  | 10.234.98.164:31115 | 0.303706  | 0.0761 | 0.4328 | 0.1289 | 1.8572 |          7.54952 |    15 |
    | MyTrainable_a8263fc6 | RUNNING  | 10.234.98.164:31117 | 0.929276  | 0.158  | 0.3417 | 0.4865 | 1.6307 |          7.0501  |    14 |
    | MyTrainable_a8267914 | RUNNING  | 10.234.98.164:31111 | 0.068426  | 0.0319 | 0.1147 | 0.9585 | 1.9603 |          7.0477  |    14 |
    | MyTrainable_a826b7bc | RUNNING  | 10.234.98.164:31112 | 0.729127  | 0.0748 | 0.1784 | 0.1797 | 1.7161 |          7.05715 |    14 |
    +----------------------+----------+---------------------+-----------+--------+--------+--------+--------+------------------+-------+

Note that columns will be hidden if they are completely empty. The output can be configured in various ways by
instantiating a ``CLIReporter`` instance (or ``JupyterNotebookReporter`` if you're using jupyter notebook).
Here's an example:

.. TODO: test these snippets

.. code-block:: python

    from ray.tune import CLIReporter

    # Limit the number of rows.
    reporter = CLIReporter(max_progress_rows=10)
    # Add a custom metric column, in addition to the default metrics.
    # Note that this must be a metric that is returned in your training results.
    reporter.add_metric_column("custom_metric")
    tuner = tune.Tuner(my_trainable, run_config=air.RunConfig(progress_reporter=reporter))
    results = tuner.fit()

Extending ``CLIReporter`` lets you control reporting frequency. For example:

.. code-block:: python

    from ray.tune.experiment.trial import Trial

    class ExperimentTerminationReporter(CLIReporter):
        def should_report(self, trials, done=False):
            """Reports only on experiment termination."""
            return done

    tuner = tune.Tuner(my_trainable, run_config=air.RunConfig(progress_reporter=ExperimentTerminationReporter()))
    results = tuner.fit()

    class TrialTerminationReporter(CLIReporter):
        def __init__(self):
            super(TrialTerminationReporter, self).__init__()
            self.num_terminated = 0

        def should_report(self, trials, done=False):
            """Reports only on trial termination events."""
            old_num_terminated = self.num_terminated
            self.num_terminated = len([t for t in trials if t.status == Trial.TERMINATED])
            return self.num_terminated > old_num_terminated

    tuner = tune.Tuner(my_trainable, run_config=air.RunConfig(progress_reporter=TrialTerminationReporter()))
    results = tuner.fit()

The default reporting style can also be overridden more broadly by extending the ``ProgressReporter`` interface directly. Note that you can print to any output stream, file etc.

.. code-block:: python

    from ray.tune import ProgressReporter

    class CustomReporter(ProgressReporter):

        def should_report(self, trials, done=False):
            return True

        def report(self, trials, *sys_info):
            print(*sys_info)
            print("\n".join([str(trial) for trial in trials]))

    tuner = tune.Tuner(my_trainable, run_config=air.RunConfig(progress_reporter=CustomReporter()))
    results = tuner.fit()


CLIReporter
-----------

.. autoclass:: ray.tune.CLIReporter
    :members: add_metric_column

JupyterNotebookReporter
-----------------------

.. autoclass:: ray.tune.JupyterNotebookReporter
    :members: add_metric_column


ProgressReporter
----------------

.. autoclass:: ray.tune.ProgressReporter
    :members:
