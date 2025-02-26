Tune Experiment Tracking Examples
---------------------------------

.. raw:: html

    <a id="try-anyscale-quickstart-ray-tune-experiment-tracking" target="_blank" href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-tune-experiment-tracking">
      <img src="../../_static/img/run-on-anyscale.svg" alt="Run on Anyscale" />
      <br/><br/>
    </a>

.. toctree::
    :hidden:

    Weights & Biases Example <tune-wandb>
    MLflow Example <tune-mlflow>
    Aim Example <tune-aim>
    Comet Example <tune-comet>


Ray Tune integrates with some popular Experiment tracking and management tools,
such as CometML, or Weights & Biases. If you're interested in learning how
to use Ray Tune with Tensorboard, you can find more information in our
:ref:`Guide to logging and outputs <tune-logging>`.

.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3


    .. grid-item-card::
        :img-top:  /images/aim_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tune-aim-ref

            Using Aim with Ray Tune For Experiment Management

    .. grid-item-card::
        :img-top: /images/comet_logo_full.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tune-comet-ref

            Using Comet with Ray Tune For Experiment Management

    .. grid-item-card::
        :img-top: /images/wandb_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tune-wandb-ref

            Tracking Your Experiment Process Weights & Biases

    .. grid-item-card::
        :img-top: /images/mlflow.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: tune-mlflow-ref

            Using MLflow Tracking & AutoLogging with Tune
