.. _air-guides:

===========
User Guides
===========

.. _air-feature-guide:

AIR User Guides
---------------

.. panels::
    :container: text-center
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top:  /ray-air/images/preprocessors.svg

    .. https://docs.google.com/drawings/d/1ZIbsXv5vvwTVIEr2aooKxuYJ_VL7-8VMNlRinAiPaTI/edit

    +++
    .. link-button:: /ray-air/preprocessors
        :type: ref
        :text: Using Preprocessors
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /ray-air/images/train-icon.svg

    .. https://docs.google.com/drawings/d/15SXGHbKPWdrzx3aTAIFcO2uh_s6Q7jLU03UMuwKSzzM/edit

    +++
    .. link-button:: trainer
        :type: ref
        :text: Using Trainers
        :classes: btn-link btn-block stretched-link

    ---
    :img-top:  /ray-air/images/ingest-icon.svg

    .. https://docs.google.com/drawings/d/10GZE_6s6ss8PSxLYyzcbj6yEalWO4N7MS7ao8KO7ne0/edit

    +++
    .. link-button:: air-ingest
        :type: ref
        :text: Configuring Training Datasets
        :classes: btn-link btn-block stretched-link

    ---
    :img-top:  /ray-air/images/tuner.svg

    .. https://docs.google.com/drawings/d/1yMd12iMkyo6DGrFoET1TIlKfFnXX9dfh2u3GSdTz6W4/edit

    +++
    .. link-button:: /ray-air/tuner
        :type: ref
        :text: Configuring Hyperparameter Tuning
        :classes: btn-link btn-block stretched-link

    ---
    :img-top:  /ray-air/images/predictors.png

    .. https://docs.google.com/presentation/d/1jfkQk0tGqgkLgl10vp4-xjcbYG9EEtlZV_Vnve_NenQ/edit#slide=id.g131c21f5e88_0_549

    +++
    .. link-button:: predictors
        :type: ref
        :text: Using Predictors for Inference
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /ray-air/images/serve-icon.svg

    .. https://docs.google.com/drawings/d/1-rg77bV-vEMURXZw5_mIOUFM3FObIIYbFOiYzFJW_68/edit

    +++
    .. link-button:: /ray-air/examples/serving_guide
        :type: ref
        :text: Deploying Predictors with Serve
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /ray-air/images/air-deploy.svg

    .. https://docs.google.com/drawings/d/1ja1RfNCEFn50B9FHWSemUzwhtPAmVyoak1JqEJUmxs4/edit

    +++
    .. link-button:: air-deployment
        :type: ref
        :text: How to Deploy AIR
        :classes: btn-link btn-block stretched-link

.. _air-env-vars:

Environment variables
---------------------

Some behavior of Ray AIR can be controlled using environment variables.

Please also see the :ref:`Ray Tune environment variables <tune-env-vars>`.

- **RAY_AIR_FULL_TRACEBACKS**: If set to 1, will print full tracebacks for training functions,
  including internal code paths. Otherwise, abbreviated tracebacks that only show user code
  are printed. Defaults to 0 (disabled).
