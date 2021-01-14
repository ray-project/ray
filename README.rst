.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

.. image:: https://readthedocs.org/projects/ray/badge/?version=master
    :target: http://docs.ray.io/en/master/?badge=master

.. image:: https://img.shields.io/badge/Ray-Join%20Slack-blue
    :target: https://forms.gle/9TSdDYUgxYs8SA9e8

.. image:: https://img.shields.io/badge/Discuss-Ask%20Questions-blue
    :target: https://discuss.ray.io/

|


**Ray provides a simple, universal API for building distributed applications.**

Ray is packaged with the following libraries for accelerating machine learning workloads:

- `Tune`_: Scalable Hyperparameter Tuning
- `RLlib`_: Scalable Reinforcement Learning
- `RaySGD <https://docs.ray.io/en/master/raysgd/raysgd.html>`__: Distributed Training Wrappers
- `Ray Serve`_: Scalable and Programmable Serving

There are also many `community integrations <https://docs.ray.io/en/master/ray-libraries.html>`_ with Ray, including `Dask`_, `MARS`_, `Modin`_, `Horovod`_, `Hugging Face`_, `Scikit-learn`_, and others. Check out the `full list of Ray distributed libraries here <https://docs.ray.io/en/master/ray-libraries.html>`_.

Install Ray with: ``pip install ray``. For nightly wheels, see the
`Installation page <https://docs.ray.io/en/master/installation.html>`__.

.. _`Modin`: https://github.com/modin-project/modin
.. _`Hugging Face`: https://huggingface.co/transformers/main_classes/trainer.html#transformers.Trainer.hyperparameter_search
.. _`MARS`: https://docs.ray.io/en/master/mars-on-ray.html
.. _`Dask`: https://docs.ray.io/en/master/dask-on-ray.html
.. _`Horovod`: https://horovod.readthedocs.io/en/stable/ray_include.html
.. _`Scikit-learn`: joblib.html



Quick Start
-----------

Execute Python functions in parallel.

.. code-block:: python

    import ray
    ray.init()

    @ray.remote
    def f(x):
        return x * x

    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures))

To use Ray's actor model:

.. code-block:: python


    import ray
    ray.init()

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 1

        def read(self):
            return self.n

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures))


Ray programs can run on a single machine, and can also seamlessly scale to large clusters. To execute the above Ray script in the cloud, just download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__, and run:

``ray submit [CLUSTER.YAML] example.py --start``

Read more about `launching clusters <https://docs.ray.io/en/master/cluster/index.html>`_.

Tune Quick Start
----------------

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/tune-wide.png

`Tune`_ is a library for hyperparameter tuning at any scale.

- Launch a multi-node distributed hyperparameter sweep in less than 10 lines of code.
- Supports any deep learning framework, including PyTorch, `PyTorch Lightning <https://github.com/williamFalcon/pytorch-lightning>`_, TensorFlow, and Keras.
- Visualize results with `TensorBoard <https://www.tensorflow.org/get_started/summaries_and_tensorboard>`__.
- Choose among scalable SOTA algorithms such as `Population Based Training (PBT)`_, `Vizier's Median Stopping Rule`_, `HyperBand/ASHA`_.
- Tune integrates with many optimization libraries such as `Facebook Ax <http://ax.dev>`_, `HyperOpt <https://github.com/hyperopt/hyperopt>`_, and `Bayesian Optimization <https://github.com/fmfn/BayesianOptimization>`_ and enables you to scale them transparently.

To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install "ray[tune]"


This example runs a parallel grid search to optimize an example objective function.

.. code-block:: python


    from ray import tune


    def objective(step, alpha, beta):
        return (0.1 + alpha * step / 100)**(-1) + beta * 0.1


    def training_function(config):
        # Hyperparameters
        alpha, beta = config["alpha"], config["beta"]
        for step in range(10):
            # Iterative training function - can be any arbitrary training procedure.
            intermediate_score = objective(step, alpha, beta)
            # Feed the score back back to Tune.
            tune.report(mean_loss=intermediate_score)


    analysis = tune.run(
        training_function,
        config={
            "alpha": tune.grid_search([0.001, 0.01, 0.1]),
            "beta": tune.choice([1, 2, 3])
        })

    print("Best config: ", analysis.get_best_config(metric="mean_loss"))

    # Get a dataframe for analyzing trial results.
    df = analysis.results_df

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results

.. _`Tune`: https://docs.ray.io/en/master/tune.html
.. _`Population Based Training (PBT)`: https://docs.ray.io/en/master/tune-schedulers.html#population-based-training-pbt
.. _`Vizier's Median Stopping Rule`: https://docs.ray.io/en/master/tune-schedulers.html#median-stopping-rule
.. _`HyperBand/ASHA`: https://docs.ray.io/en/master/tune-schedulers.html#asynchronous-hyperband

RLlib Quick Start
-----------------

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/rllib-wide.jpg

`RLlib`_ is an open-source library for reinforcement learning built on top of Ray that offers both high scalability and a unified API for a variety of applications.

.. code-block:: bash

  pip install tensorflow  # or tensorflow-gpu
  pip install "ray[rllib]"

.. code-block:: python

    import gym
    from gym.spaces import Discrete, Box
    from ray import tune

    class SimpleCorridor(gym.Env):
        def __init__(self, config):
            self.end_pos = config["corridor_length"]
            self.cur_pos = 0
            self.action_space = Discrete(2)
            self.observation_space = Box(0.0, self.end_pos, shape=(1, ))

        def reset(self):
            self.cur_pos = 0
            return [self.cur_pos]

        def step(self, action):
            if action == 0 and self.cur_pos > 0:
                self.cur_pos -= 1
            elif action == 1:
                self.cur_pos += 1
            done = self.cur_pos >= self.end_pos
            return [self.cur_pos], 1 if done else 0, done, {}

    tune.run(
        "PPO",
        config={
            "env": SimpleCorridor,
            "num_workers": 4,
            "env_config": {"corridor_length": 5}})

.. _`RLlib`: https://docs.ray.io/en/master/rllib.html


Ray Serve Quick Start
---------------------

.. image:: https://raw.githubusercontent.com/ray-project/ray/master/doc/source/serve/logo.svg
  :width: 400

`Ray Serve`_ is a scalable model-serving library built on Ray. It is:

- Framework Agnostic: Use the same toolkit to serve everything from deep
  learning models built with frameworks like PyTorch or Tensorflow & Keras
  to Scikit-Learn models or arbitrary business logic.
- Python First: Configure your model serving with pure Python code - no more
  YAMLs or JSON configs.
- Performance Oriented: Turn on batching, pipelining, and GPU acceleration to
  increase the throughput of your model.
- Composition Native: Allow you to create "model pipelines" by composing multiple
  models together to drive a single prediction.
- Horizontally Scalable: Serve can linearly scale as you add more machines. Enable
  your ML-powered service to handle growing traffic.

To run this example, you will need to install the following:

.. code-block:: bash

    $ pip install scikit-learn
    $ pip install "ray[serve]"

This example runs serves a scikit-learn gradient boosting classifier.

.. code-block:: python

    from ray import serve
    import pickle
    import requests
    from sklearn.datasets import load_iris
    from sklearn.ensemble import GradientBoostingClassifier

    # Train model
    iris_dataset = load_iris()
    model = GradientBoostingClassifier()
    model.fit(iris_dataset["data"], iris_dataset["target"])

    # Define Ray Serve model,
    class BoostingModel:
        def __init__(self):
            self.model = model
            self.label_list = iris_dataset["target_names"].tolist()

        def __call__(self, flask_request):
            payload = flask_request.json["vector"]
            print("Worker: received flask request with data", payload)

            prediction = self.model.predict([payload])[0]
            human_name = self.label_list[prediction]
            return {"result": human_name}


    # Deploy model
    client = serve.start()
    client.create_backend("iris:v1", BoostingModel)
    client.create_endpoint("iris_classifier", backend="iris:v1", route="/iris")

    # Query it!
    sample_request_input = {"vector": [1.2, 1.0, 1.1, 0.9]}
    response = requests.get("http://localhost:8000/iris", json=sample_request_input)
    print(response.text)
    # Result:
    # {
    #  "result": "versicolor"
    # }


.. _`Ray Serve`: https://docs.ray.io/en/master/serve/index.html

More Information
----------------

- `Documentation`_
- `Tutorial`_
- `Blog`_
- `Ray 1.0 Architecture whitepaper`_ **(new)**
- `Ray Design Patterns`_ **(new)**
- `RLlib paper`_
- `Tune paper`_

*Older documents:*

- `Ray paper`_
- `Ray HotOS paper`_
- `Blog (old)`_

.. _`Documentation`: http://docs.ray.io/en/master/index.html
.. _`Tutorial`: https://github.com/ray-project/tutorial
.. _`Blog (old)`: https://ray-project.github.io/
.. _`Blog`: https://medium.com/distributed-computing-with-ray
.. _`Ray 1.0 Architecture whitepaper`: https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview
.. _`Ray Design Patterns`: https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924
.. _`RLlib paper`: https://arxiv.org/abs/1712.09381
.. _`Tune paper`: https://arxiv.org/abs/1807.05118

Getting Involved
----------------

- `Community Slack`_: Join our Slack workspace.
- `Forum`_: For discussions about development, questions about usage, and feature requests.
- `GitHub Issues`_: For reporting bugs.
- `Twitter`_: Follow updates on Twitter.
- `Meetup Group`_: Join our meetup group.
- `StackOverflow`_: For questions about how to use Ray.

.. _`Forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Meetup Group`: https://www.meetup.com/Bay-Area-Ray-Meetup/
.. _`Community Slack`: https://forms.gle/9TSdDYUgxYs8SA9e8
.. _`Twitter`: https://twitter.com/raydistributed
