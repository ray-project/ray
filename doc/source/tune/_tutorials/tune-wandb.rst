.. _tune-wandb:

Using Weights & Biases with Tune
================================

`Weights & Biases <https://www.wandb.ai/>`_ (Wandb) is a tool for experiment
tracking, model optimizaton, and dataset versioning. It is very popular
in the machine learning and data science community for its superb visualization
tools.

.. image:: /images/wandb_logo_full.png
  :height: 80px
  :alt: Weights & Biases
  :align: center
  :target: https://www.wandb.ai/

Ray Tune currently offers two lightweight integrations for Weights & Biases.
One is the :ref:`WandbLoggerCallback <tune-wandb-logger>`, which automatically logs
metrics reported to Tune to the Wandb API.

The other one is the :ref:`@wandb_mixin <tune-wandb-mixin>` decorator, which can be
used with the function API. It automatically
initializes the Wandb API with Tune's training information. You can just use the
Wandb API like you would normally do, e.g. using ``wandb.log()`` to log your training
process.

Please :doc:`see here for a full example </tune/examples/wandb_example>`.

.. _tune-wandb-logger:

.. autoclass:: ray.tune.integration.wandb.WandbLoggerCallback
   :noindex:

.. _tune-wandb-mixin:

.. autofunction:: ray.tune.integration.wandb.wandb_mixin
   :noindex:
