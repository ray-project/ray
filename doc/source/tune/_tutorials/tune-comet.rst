.. _tune-comet:

Using Comet with Tune
================================

`Comet <https://www.comet.ml/site/>`_ is a tool to manage and optimize the
entire ML lifecycle, from experiment tracking, model optimization and dataset
versioning to model production monitoring.

.. image:: /images/comet_logo_full.png
  :height: 80px
  :alt: Comet
  :align: center
  :target: https://www.comet.ml/site/

Ray Tune offers an integration with Comet through the :ref:`CometLoggerCallback <tune-comet-logger>`, which automatically logs
metrics and parameters reported to Tune to the Comet UI.

Please :doc:`see here for a full example </tune/examples/comet_example>`.

.. _tune-comet-logger:

.. autoclass:: ray.tune.integration.comet.CometLoggerCallback
   :noindex:
