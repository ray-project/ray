.. _tune-mlflow:

Using MLflow with Tune
======================

`MLflow <https://mlflow.org/>`_ is an open source platform to manage the ML lifecycle, including experimentation,
reproducibility, deployment, and a central model registry. It currently offers four components, including
MLflow Tracking to record and query experiments, including code, data, config, and results.

.. image:: /images/mlflow.png
  :height: 80px
  :alt: MLflow
  :align: center
  :target: https://www.mlflow.org/

Ray Tune currently offers two lightweight integrations for MLflow Tracking.
One is the :ref:`MLflowLoggerCallback <tune-mlflow-logger>`, which automatically logs
metrics reported to Tune to the MLflow Tracking API.

The other one is the :ref:`@mlflow_mixin <tune-mlflow-mixin>` decorator, which can be
used with the function API. It automatically
initializes the MLflow API with Tune's training information and creates a run for each Tune trial.
Then within your training function, you can just use the
MLflow like you would normally do, e.g. using ``mlflow.log_metrics()`` or even ``mlflow.autolog()``
to log to your training process.

Please :doc:`see here </tune/examples/mlflow_example>` for a full example on how you can use either the
MLflowLoggerCallback or the mlflow_mixin.

MLflow AutoLogging
------------------
You can also check out :doc:`here </tune/examples/mlflow_ptl_example>` for an example on how you can leverage MLflow
autologging, in this case with Pytorch Lightning

MLflow Logger API
-----------------
.. _tune-mlflow-logger:

.. autoclass:: ray.tune.integration.mlflow.MLflowLoggerCallback
   :noindex:

MLflow Mixin API
----------------
.. _tune-mlflow-mixin:

.. autofunction:: ray.tune.integration.mlflow.mlflow_mixin
   :noindex:
