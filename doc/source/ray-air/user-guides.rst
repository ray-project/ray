.. _air-guides:

===========
User Guides
===========

.. _air-feature-guide:

AIR User Guides
---------------

.. grid:: 3
    :gutter: 2
    :class-container: container pb-4

    .. grid-item-card::
        :img-top: /ray-air/images/preprocessors.svg
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: /ray-air/preprocessors
            :color: primary
            :outline:
            :expand:

            Using Preprocessors

    .. grid-item-card::
        :img-top: /ray-air/images/train-icon.svg
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: trainers
            :color: primary
            :outline:
            :expand:

            Using Trainers

    .. grid-item-card::
        :img-top: /ray-air/images/ingest-icon.svg
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: air-ingest
            :color: primary
            :outline:
            :expand:

            Configuring Training Datasets

    .. grid-item-card::
        :img-top: /ray-air/images/tuner.svg
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: /ray-air/tuner
            :color: primary
            :outline:
            :expand:

            Configuring Hyperparameter Tuning

    .. grid-item-card::
        :img-top:  /ray-air/images/predictors.png
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: predictors
            :color: primary
            :outline:
            :expand:

            Using Predictors for Inference

    .. grid-item-card::
        :img-top: /ray-air/images/serve-icon.svg
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: /ray-air/examples/serving_guide
            :color: primary
            :outline:
            :expand:

            Deploying Predictors with Serve

    .. grid-item-card::
        :img-top: /ray-air/images/air-deploy.svg
        :class-img-top: pt-5 w-75 d-block mx-auto fixed-height-img

        +++
        .. button-ref:: air-deployment
            :color: primary
            :outline:
            :expand:

            How to Deploy AIR


.. _air-env-vars:

Environment variables
---------------------

Some behavior of Ray AIR can be controlled using environment variables.

Please also see the :ref:`Ray Tune environment variables <tune-env-vars>`.

- **RAY_AIR_FULL_TRACEBACKS**: If set to 1, will print full tracebacks for training functions,
  including internal code paths. Otherwise, abbreviated tracebacks that only show user code
  are printed. Defaults to 0 (disabled).
- **RAY_AIR_NEW_OUTPUT**: If set to 0, this disables
  the :ref:`experimental new console output <air-experimental-new-output>`.
- **RAY_AIR_RICH_LAYOUT**: If set to 1, this enables
  the :ref:`stick table layout <air-experimental-rich>`
  (only available for Ray Tune).

.. _air-multi-tenancy:

Running multiple AIR jobs concurrently on a single cluster
----------------------------------------------------------
Running multiple AIR training or tuning jobs at the same
time on a single cluster is not officially supported.
We don't test this workflow
and recommend the use of multiple smaller clusters
instead.

If you still want to do this, refer to
the
:ref:`Ray Tune multi-tenancy docs <tune-multi-tenancy>`
for potential pitfalls.

.. _air-experimental-overview:

Experimental features in Ray 2.5+
---------------------------------
Starting in Ray 2.5, some experimental
features are enabled by default.

Experimental features are enabled to allow for feedback
from users. Every experimental feature can be disabled
by setting an environment variable. Some features are
not ready for general testing and can only be *enabled* using an
environment variable.

Please see the :ref:`experimental features <air-experimental-features>`
page for more details on the current features and how to enable
or disable them.
