.. currentmodule:: ray

.. _air-experiment-tracking-integrations:
.. _air-builtin-callbacks:

Experiment Tracking Integrations
================================

Comet (air.integrations.comet)
------------------------------

.. autosummary::
    :toctree: doc/

    ~air.integrations.comet.CometLoggerCallback

.. seealso::

    :ref:`See here for an example. <tune-comet-ref>`


.. _air-integration-mlflow:

MLflow (air.integrations.mlflow)
--------------------------------

.. autosummary::
    :toctree: doc/

    ~air.integrations.mlflow.MLflowLoggerCallback
    ~air.integrations.mlflow.setup_mlflow

.. seealso::

    :ref:`See here for an example. <tune-mlflow-ref>`


.. _air-integration-wandb:

Weights and Biases (air.integrations.wandb)
-------------------------------------------

.. autosummary::
    :toctree: doc/

    ~air.integrations.wandb.WandbLoggerCallback
    ~air.integrations.wandb.setup_wandb

.. seealso::

    :ref:`See here for an example. <tune-wandb-ref>`
