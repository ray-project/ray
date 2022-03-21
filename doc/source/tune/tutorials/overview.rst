.. _tune-guides:

===========
User Guides
===========

.. tip:: We'd love to hear your feedback on using Tune - `get in touch <https://forms.gle/PTRvGLbKRdUfuzQo9>`_!

In this section, you can find material on how to use Tune and its various features.
You can follow our :ref:`How-To Guides<tune-recipes>`, :ref:`Tune Feature Guides<tune-feature-guides>`, or
go through some :ref:`Exercises<tune-exercises>`,  to get started.


.. _tune-recipes:

Practical How-To Guides
-----------------------

.. panels::
    :container: container pb-4 full-width
    :column: col-md-3 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/tune-sklearn.png

    +++
    .. link-button:: tune-sklearn
        :type: ref
        :text: How To Use Tune's Scikit-Learn Adapters?
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/pytorch_logo.png

    +++
    .. link-button:: tune-pytorch-cifar-ref
        :type: ref
        :text: How To Use Tune With PyTorch Models?
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/pytorch_lightning_small.png

    +++
    .. link-button:: tune-pytorch-lightning-ref
        :type: ref
        :text: How To Tune PyTorch Lightning Models
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/serve.png

    +++
    .. link-button:: tune-serve-integration-mnist
        :type: ref
        :text: Model Selection & Serving With Ray Serve
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/xgboost_logo.png

    +++
    .. link-button:: tune-xgboost-ref
        :type: ref
        :text: A Guide To Tuning XGBoost Parameters With Tune
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/wandb_logo.png

    +++
    .. link-button:: tune-wandb-ref
        :type: ref
        :text: Tracking Your Experiment Process Weights & Biases
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/mlflow.png

    +++
    .. link-button:: tune-mlflow-ref
        :type: ref
        :text: Using MLflow Tracking & AutoLogging with Tune
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/comet_logo_full.png

    +++
    .. link-button:: tune-comet-ref
        :type: ref
        :text: Using Comet with Ray Tune For Experiment Management
        :classes: btn-link btn-block stretched-link


.. _tune-feature-guides:

Tune Feature Guides
-------------------

.. panels::
    :container: container pb-4 full-width
    :column: col-md-3 px-2 py-2
    :img-top-cls: pt-5 w-50 d-block mx-auto

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-stopping
        :type: ref
        :text: A Guide To Stopping and Resuming Tune Experiments
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-metrics
        :type: ref
        :text: Using Callbacks and Metrics in Tune
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-output
        :type: ref
        :text: How To Log Tune Runs
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-resources
        :type: ref
        :text: Using Resources (GPUs, Parallel & Distributed Runs)
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-checkpoints
        :type: ref
        :text: Using Checkpoints For Your Experiments
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-lifecycle
        :type: ref
        :text: How does Tune work?
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-advanced-tutorial
        :type: ref
        :text: A simple guide to Population-based Training
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-distributed
        :type: ref
        :text: A Guide To Distributed Hyperparameter Tuning
        :classes: btn-link btn-block stretched-link


.. _tune-exercises:

Exercises
---------

Learn how to use Tune in your browser with the following Colab-based exercises.

.. raw:: html

    <table>
      <tr>
        <th class="tune-colab">Exercise Description</th>
        <th class="tune-colab">Library</th>
        <th class="tune-colab">Colab Link</th>
      </tr>
      <tr>
        <td class="tune-colab">Basics of using Tune.</td>
        <td class="tune-colab">TF/Keras</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_1_basics.ipynb" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>

      <tr>
        <td class="tune-colab">Using Search algorithms and Trial Schedulers to optimize your model.</td>
        <td class="tune-colab">Pytorch</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_2_optimize.ipynb" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>

      <tr>
        <td class="tune-colab">Using Population-Based Training (PBT).</td>
        <td class="tune-colab">Pytorch</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_3_pbt.ipynb" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>

      <tr>
        <td class="tune-colab">Fine-tuning Huggingface Transformers with PBT.</td>
        <td class="tune-colab">Huggingface Transformers/Pytorch</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/drive/1tQgAKgcKQzheoh503OzhS4N9NtfFgmjF?usp=sharing" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>

      <tr>
        <td class="tune-colab">Logging Tune Runs to Comet ML.</td>
        <td class="tune-colab">Comet</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/drive/1dp3VwVoAH1acn_kG7RuT62mICnOqxU1z?usp=sharing" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>
    </table>

Tutorial source files `can be found here <https://github.com/ray-project/tutorial>`_.

