Execution (tune.run, tune.Experiment)
=====================================

.. _tune-run-ref:

tune.run
--------

.. autofunction:: ray.tune.run

tune.run_experiments
--------------------

.. autofunction:: ray.tune.run_experiments

tune.Experiment
---------------

.. autofunction:: ray.tune.Experiment

.. _tune-stop-ref:

Stopper (tune.Stopper)
----------------------

.. autoclass:: ray.tune.Stopper
    :members: __call__, stop_all

.. _tune-sklearn-docs:

Scikit-Learn integration (tune.sklearn)
---------------------------------------

.. _tunegridsearchcv-docs:

.. autoclass:: ray.tune.integration.sklearn.TuneGridSearchCV
	:inherited-members:

.. _tunesearchcv-docs:

.. autoclass:: ray.tune.integration.sklearn.TuneSearchCV
	:inherited-members:
