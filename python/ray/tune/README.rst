Ray.tune: Efficient distributed hyperparameter search
=====================================================

Ray.tune is a hyperparameter tuning tool for long-running tasks such as RL and deep learning training.

User documentation can be `found here <https://github.com/ray-project/ray/blob/master/doc/source/tune.rst>`__.

Implementation overview
-----------------------

At a high level, Ray.tune provides 
There are several components:
  - TrialRunner
  - TrialScheduler
  - VariantGenerator
