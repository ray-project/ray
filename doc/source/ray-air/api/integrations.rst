.. _air-integrations:

External library integrations for Ray AIR
=========================================

.. currentmodule:: ray

.. _air-monitoring-integrations:
.. _air-builtin-callbacks:

Tune Experiment Monitoring Integrations
---------------------------------------

Comet (air.integrations.comet)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.comet.CometLoggerCallback

.. seealso::

    :ref:`See here for an example. <tune-comet-ref>`


.. _tune-integration-mlflow:

MLflow (air.integrations.mlflow)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.mlflow.MLflowLoggerCallback
    ~air.integrations.mlflow.setup_mlflow

.. seealso::

    :ref:`See here for an example. <tune-mlflow-ref>`


.. _tune-integration-wandb:

Weights and Biases (air.integrations.wandb)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.wandb.WandbLoggerCallback
    ~air.integrations.wandb.setup_wandb

.. seealso::

    :ref:`See here for an example. <tune-wandb-ref>`


Integrations with ML Libraries
------------------------------

.. _tune-integration-keras:

Keras (air.integrations.keras)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.keras.ReportCheckpointCallback

.. seealso::

    :ref:`See here for an example. <air-convert-tf-to-air>`
