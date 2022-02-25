.. _tune-examples-ref:

========
Examples
========

.. tip:: Check out :ref:`the Tune User Guides <tune-guides>` To learn more about Tune's features in depth.

.. _tune-recipes:

Practical How-To Guides
-----------------------

Ray Tune integrates with many popular machine learning frameworks.
Here you find a few practical examples showing you how to tune your models.
At the end of these guides you will often find links to even more examples.

.. panels::
    :container: container pb-4
    :column: col-md-4 px-2 py-2
    :img-top-cls: pt-5 w-75 d-block mx-auto

    ---
    :img-top: /images/tune-sklearn.png

    +++
    .. link-button:: tune-sklearn
        :type: ref
        :text: How To Use Tune's Scikit-Learn Adapters?
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/keras.png

    +++
    .. link-button:: tune-mnist-keras
        :type: ref
        :text: How To Use Tune With Keras & TF Models
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/pytorch_logo.png

    +++
    .. link-button:: tune-pytorch-cifar-ref
        :type: ref
        :text: How To Use Tune With PyTorch Models
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/pytorch_lightning_small.png

    +++
    .. link-button:: tune-pytorch-lightning-ref
        :type: ref
        :text: How To Tune PyTorch Lightning Models
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/mxnet_logo.png

    +++
    .. link-button:: tune-mxnet-example
        :type: ref
        :text: How To Tune MXNet Models
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/serve.svg

    +++
    .. link-button:: tune-serve-integration-mnist
        :type: ref
        :text: Model Selection & Serving With Ray Serve
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /rllib/images/rllib-logo.png

    +++
    .. link-button:: tune-rllib-example
        :type: ref
        :text: Tuning RL Experiments With Ray Tune & Ray Serve
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/xgboost_logo.png

    +++
    .. link-button:: tune-xgboost-ref
        :type: ref
        :text: A Guide To Tuning XGBoost Parameters With Tune
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/lightgbm_logo.png

    +++
    .. link-button:: tune-lightgbm-example
        :type: ref
        :text: A Guide To Tuning LightGBM Parameters With Tune
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/horovod.png

    +++
    .. link-button:: tune-horovod-example
        :type: ref
        :text: A Guide To Tuning Horovod Parameters With Tune
        :classes: btn-link btn-block stretched-link

    ---
    :img-top: /images/hugging.png

    +++
    .. link-button:: tune-huggingface-example
        :type: ref
        :text: A Guide To Tuning Huggingface Transformers With Tune
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


Search Algorithm Examples
-------------------------

.. TODO: make these panels with logos!

- :doc:`/tune/examples/includes/ax_example`:
  Example script showing usage of :ref:`AxSearch <tune-ax>` [`Ax website <https://ax.dev/>`__]
- :doc:`/tune/examples/includes/dragonfly_example`:
  Example script showing usage of :ref:`DragonflySearch <Dragonfly>` [`Dragonfly website <https://dragonfly-opt.readthedocs.io/>`__]
- :doc:`/tune/examples/includes/skopt_example`:
  Example script showing usage of :ref:`SkoptSearch <skopt>` [`Scikit-Optimize website <https://scikit-optimize.github.io>`__]
- :doc:`/tune/examples/hyperopt_example`:
  Example script showing usage of :ref:`HyperOptSearch <tune-hyperopt>` [`HyperOpt website <http://hyperopt.github.io/hyperopt>`__]
- :doc:`/tune/examples/includes/hyperopt_conditional_search_space_example`:
  Example script showing usage of :ref:`HyperOptSearch <tune-hyperopt>` [`HyperOpt website <http://hyperopt.github.io/hyperopt>`__] with a conditional search space
- :doc:`/tune/examples/includes/bayesopt_example`:
  Example script showing usage of :ref:`BayesOptSearch <bayesopt>` [`BayesianOptimization website <https://github.com/fmfn/BayesianOptimization>`__]
- :doc:`/tune/examples/includes/blendsearch_example`:
  Example script showing usage of :ref:`BlendSearch <BlendSearch>` [`BlendSearch website <https://github.com/microsoft/FLAML/tree/main/flaml/tune>`__]
- :doc:`/tune/examples/includes/cfo_example`:
  Example script showing usage of :ref:`CFO <CFO>` [`CFO website <https://github.com/microsoft/FLAML/tree/main/flaml/tune>`__]
- :doc:`/tune/examples/includes/bohb_example`:
  Example script showing usage of :ref:`TuneBOHB <suggest-TuneBOHB>` [`BOHB website <https://github.com/automl/HpBandSter>`__]
- :doc:`/tune/examples/includes/nevergrad_example`:
  Example script showing usage of :ref:`NevergradSearch <nevergrad>` [`Nevergrad website <https://github.com/facebookresearch/nevergrad>`__]
- :doc:`/tune/examples/includes/optuna_example`:
  Example script showing usage of :ref:`OptunaSearch <tune-optuna>` [`Optuna website <https://optuna.org/>`__]
- :doc:`/tune/examples/includes/optuna_define_by_run_example`:
  Example script showing usage of :ref:`OptunaSearch <tune-optuna>` [`Optuna website <https://optuna.org/>`__] with a define-by-run function
- :doc:`/tune/examples/includes/optuna_multiobjective_example`:
  Example script showing usage of :ref:`OptunaSearch <tune-optuna>` [`Optuna website <https://optuna.org/>`__] for multi-objective optimization
- :doc:`/tune/examples/includes/zoopt_example`:
  Example script showing usage of :ref:`ZOOptSearch <zoopt>` [`ZOOpt website <https://github.com/polixir/ZOOpt>`__]
- :doc:`/tune/examples/includes/sigopt_example`:
  Example script showing usage of :ref:`SigOptSearch <sigopt>` [`SigOpt website <https://sigopt.com/>`__]
- :doc:`/tune/examples/includes/hebo_example`:
  Example script showing usage of :ref:`HEBOSearch <tune-hebo>` [`HEBO website <https://github.com/huawei-noah/HEBO/tree/master/HEBO>`__]
- :doc:`/tune/examples/includes/sigopt_multi_objective_example`:
  Example using Sigopt's multi-objective functionality (contributed).
- :doc:`/tune/examples/includes/sigopt_prior_beliefs_example`:
  Example using Sigopt's support for prior beliefs (contributed).


.. _tune-general-examples:

Other Examples
--------------

- :doc:`/tune/examples/includes/tune_basic_example`: Simple example for doing a basic random and grid search.
- :doc:`/tune/examples/includes/async_hyperband_example`: Example of using a simple tuning function with
  AsyncHyperBandScheduler.
- :doc:`/tune/examples/includes/hyperband_function_example`:
  Example of using a Trainable function with HyperBandScheduler.
  Also uses the AsyncHyperBandScheduler.
- :doc:`/tune/examples/includes/pbt_function`:
  Example of using the function API with a PopulationBasedTraining scheduler.
- :doc:`/tune/examples/includes/pb2_example`: Example of using the Population-based Bandits (PB2) scheduler.
- :doc:`/tune/examples/includes/logging_example`: Example of custom loggers and custom trial directory naming.
- :doc:`/tune/examples/includes/genetic_example`: Optimizing the michalewicz function using the contributed
  GeneticSearch algorithm with AsyncHyperBandScheduler.


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
