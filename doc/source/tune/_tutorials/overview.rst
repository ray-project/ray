.. _tune-guides:

===========
User Guides
===========

.. tip:: We'd love to hear your feedback on using Tune - `get in touch <https://forms.gle/PTRvGLbKRdUfuzQo9>`_!

In this section, you can find material on how to use Tune and its various features.
If any of the materials is out of date or broken, or if you'd like to add an example to this page,
feel free to raise an issue on our Github repository.

.. panels::
    :container: container pb-4 full-width
    :column: col-lg-3 px-2 py-2

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-lifecycle
        :type: ref
        :text: Go To Reference
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-lifecycle
        :type: ref
        :text: Go To Reference
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-lifecycle
        :type: ref
        :text: Go To Reference
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/tune.png

    .. link-button:: tune-lifecycle
        :type: ref
        :text: Go To Reference
        :classes: btn-link btn-block stretched-link


Take a look at any of the below tutorials to get started with Tune.

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. customgalleryitem::
   :tooltip: A deep dive into Tune's workings.
   :figure: /images/tune.png
   :description: :doc:`How does Tune work? <tune-lifecycle>`

.. customgalleryitem::
   :tooltip: A simple guide to Population-based Training
   :figure: /images/tune-pbt-small.png
   :description: :doc:`A simple guide to Population-based Training <tune-advanced-tutorial>`

.. customgalleryitem::
   :tooltip: A guide to distributed hyperparameter tuning
   :figure: /images/tune.png
   :description: :doc:`A guide to distributed hyperparameter tuning <tune-distributed>`

.. customgalleryitem::
   :tooltip: Tune's Scikit-Learn Adapters
   :figure: /images/tune-sklearn.png
   :description: :doc:`Tune's Scikit-Learn Adapters <tune-sklearn>`

.. customgalleryitem::
   :tooltip: How to use Tune with PyTorch
   :figure: /images/pytorch_logo.png
   :description: :doc:`How to use Tune with PyTorch <tune-pytorch-cifar>`

.. customgalleryitem::
   :tooltip: Tuning PyTorch Lightning modules
   :figure: /images/pytorch_lightning_small.png
   :description: :doc:`Tuning PyTorch Lightning modules <tune-pytorch-lightning>`

.. customgalleryitem::
   :tooltip: Model selection and serving with Ray Tune and Ray Serve
   :figure: /images/serve.png
   :description: :doc:`Model selection and serving with Ray Tune and Ray Serve <tune-serve-integration-mnist>`

.. customgalleryitem::
   :tooltip: Tuning XGBoost parameters.
   :figure: /images/xgboost_logo.png
   :description: :doc:`A guide to tuning XGBoost parameters with Tune <tune-xgboost>`

.. customgalleryitem::
   :tooltip: Use Weights & Biases within Tune.
   :figure: /images/wandb_logo.png
   :description: :doc:`Track your experiment process with the Weights & Biases tools <tune-wandb>`

.. customgalleryitem::
    :tooltip: Use MLflow with Ray Tune.
    :figure: /images/mlflow.png
    :description: :doc:`Log and track your hyperparameter sweep with MLflow Tracking & AutoLogging <tune-mlflow>`

.. customgalleryitem::
    :tooltip: Use Comet with Ray Tune.
    :figure: /images/comet_logo_full.png
    :description: :doc:`Log and analyze your Tune trial runs with Comet's Experiment Management Tools <tune-comet>`



.. customgalleryitem::
   :tooltip: Stopping & Resuming
   :figure: /images/tune.png
   :description: :doc:`A Guide To Stopping and Resuming Tune Experiments<tune-stopping>`

.. customgalleryitem::
   :tooltip: Callbacks & Metrics
   :figure: /images/tune.png
   :description: :doc:`Using Callbacks and Metrics in Tune<tune-metrics>`

.. customgalleryitem::
   :tooltip: Logging & Debugging
   :figure: /images/tune.png
   :description: :doc:`Logging Outputs & Debugging Tune<tune-output>`

.. customgalleryitem::
   :tooltip: Resources
   :figure: /images/tune.png
   :description: :doc:`Using Resources (GPUs, Parallel & Distributed Runs) <tune-resources>`

.. customgalleryitem::
   :tooltip: Checkpoints
   :figure: /images/tune.png
   :description: :doc:`Using Checkpoints For Your Experiments<tune-checkpoints>`


.. raw:: html

    </div>


.. toctree::
    :hidden:

    tune-advanced-tutorial.rst
    tune-distributed.rst
    tune-lifecycle.rst
    tune-mlflow.rst
    tune-pytorch-cifar.rst
    tune-pytorch-lightning.rst
    tune-serve-integration-mnist.rst
    tune-sklearn.rst
    tune-xgboost.rst
    tune-wandb.rst
    tune-comet.rst

    tune-stopping.rst
    tune-metrics.rst
    tune-output.rst
    tune-resources.rst
    tune-checkpoints.rst


Colab Exercises
---------------

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

What's Next?
-------------

Check out:

 * :doc:`/tune/examples/index`: End-to-end examples and templates for using Tune with your preferred machine learning library.
