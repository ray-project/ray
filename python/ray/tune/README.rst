Ray.tune: Hyperparameter Optimization Framework
===============================================

Ray.tune is a hyperparameter tuning framework for long-running tasks such as RL and deep learning training.

User documentation can be `found here <http://ray.readthedocs.io/en/latest/tune.html>`__.

Implementation overview
-----------------------

At a high level, Ray.tune takes in JSON experiment configs (e.g. that defines the grid or random search)
and compiles them into a number of `Trial` objects. It schedules trials on the Ray cluster using a given
`TrialScheduler` implementation (e.g. median stopping rule or HyperBand).

This is implemented as follows:

-  `variant_generator.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/suggest/variant_generator.py>`__
   parses the config and generates the trial variants.

-  `trial.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial.py>`__ manages the lifecycle
   of the Ray actor responsible for executing the trial.

-  `trial_runner.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/tune.py>`__ tracks scheduling
   state for all the trials of an experiment. TrialRunners are usually
   created automatically by ``run_experiments(experiment_json)``, which parses and starts the experiments.

-  `trial_scheduler.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/trial_scheduler.py>`__
   plugs into TrialRunner to implement custom prioritization or early stopping algorithms.
