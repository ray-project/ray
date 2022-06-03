.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

.. image:: https://readthedocs.org/projects/ray/badge/?version=master
    :target: http://docs.ray.io/en/master/?badge=master

.. image:: https://img.shields.io/badge/Ray-Join%20Slack-blue
    :target: https://forms.gle/9TSdDYUgxYs8SA9e8

.. image:: https://img.shields.io/badge/Discuss-Ask%20Questions-blue
    :target: https://discuss.ray.io/

.. image:: https://img.shields.io/twitter/follow/raydistributed.svg?style=social&logo=twitter
    :target: https://twitter.com/raydistributed

|


**Ray provides a simple, universal API for building distributed applications.**

Ray is packaged with the following libraries for accelerating machine learning workloads:

- `Tune`_: Scalable Hyperparameter Tuning
- `RLlib`_: Scalable Reinforcement Learning
- `Train`_: Distributed Deep Learning (beta)
- `Datasets`_: Distributed Data Loading and Compute

As well as libraries for taking ML and distributed apps to production:

- `Serve`_: Scalable and Programmable Serving
- `Workflows`_: Fast, Durable Application Flows (alpha)

There are also many `community integrations <https://docs.ray.io/en/master/ray-libraries.html>`_ with Ray, including `Dask`_, `MARS`_, `Modin`_, `Horovod`_, `Hugging Face`_, `Scikit-learn`_, and others. Check out the `full list of Ray distributed libraries here <https://docs.ray.io/en/master/ray-libraries.html>`_.

Install Ray with: ``pip install ray``. For nightly wheels, see the
`Installation page <https://docs.ray.io/en/master/installation.html>`__.

.. _`Modin`: https://github.com/modin-project/modin
.. _`Hugging Face`: https://huggingface.co/transformers/main_classes/trainer.html#transformers.Trainer.hyperparameter_search
.. _`MARS`: https://docs.ray.io/en/latest/data/mars-on-ray.html
.. _`Dask`: https://docs.ray.io/en/latest/data/dask-on-ray.html
.. _`Horovod`: https://horovod.readthedocs.io/en/stable/ray_include.html
.. _`Scikit-learn`: https://docs.ray.io/en/master/joblib.html
.. _`Serve`: https://docs.ray.io/en/master/serve/index.html
.. _`Datasets`: https://docs.ray.io/en/master/data/dataset.html
.. _`Workflows`: https://docs.ray.io/en/master/workflows/concepts.html
.. _`Train`: https://docs.ray.io/en/master/train/train.html


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
- Visualize results with `TensorBoard <https://www.tensorflow.org/tensorboard>`__.
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

    print("Best config: ", analysis.get_best_config(metric="mean_loss", mode="min"))

    # Get a dataframe for analyzing trial results.
    df = analysis.results_df

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results

.. _`Tune`: https://docs.ray.io/en/master/tune.html
.. _`Population Based Training (PBT)`: https://docs.ray.io/en/master/tune/api_docs/schedulers.html#population-based-training-tune-schedulers-populationbasedtraining
.. _`Vizier's Median Stopping Rule`: https://docs.ray.io/en/master/tune/api_docs/schedulers.html#median-stopping-rule-tune-schedulers-medianstoppingrule
.. _`HyperBand/ASHA`: https://docs.ray.io/en/master/tune/api_docs/schedulers.html#asha-tune-schedulers-ashascheduler

RLlib Quick Start
-----------------

.. image:: https://github.com/ray-project/ray/raw/master/doc/source/rllib/images/rllib-logo.png

`RLlib`_ is an industry-grade library for reinforcement learning (RL), built on top of Ray.
It offers high scalability and unified APIs for a
`variety of industry- and research applications <https://www.anyscale.com/event-category/ray-summit>`_.

.. code-block:: bash

    $ pip install "ray[rllib]" tensorflow  # or torch


.. Do NOT edit the following code directly in this README! Instead, edit
    the ray/rllib/examples/documentation/rllib_on_ray_readme.py script and then
    copy the new code in here:

.. code-block:: python

    import gym
    from ray.rllib.agents.ppo import PPOTrainer


    # Define your problem using python and openAI's gym API:
    class SimpleCorridor(gym.Env):
        """Corridor in which an agent must learn to move right to reach the exit.

        ---------------------
        | S | 1 | 2 | 3 | G |   S=start; G=goal; corridor_length=5
        ---------------------

        Possible actions to chose from are: 0=left; 1=right
        Observations are floats indicating the current field index, e.g. 0.0 for
        starting position, 1.0 for the field next to the starting position, etc..
        Rewards are -0.1 for all steps, except when reaching the goal (+1.0).
        """

        def __init__(self, config):
            self.end_pos = config["corridor_length"]
            self.cur_pos = 0
            self.action_space = gym.spaces.Discrete(2)  # left and right
            self.observation_space = gym.spaces.Box(0.0, self.end_pos, shape=(1,))

        def reset(self):
            """Resets the episode and returns the initial observation of the new one.
            """
            self.cur_pos = 0
            # Return initial observation.
            return [self.cur_pos]

        def step(self, action):
            """Takes a single step in the episode given `action`

            Returns:
                New observation, reward, done-flag, info-dict (empty).
            """
            # Walk left.
            if action == 0 and self.cur_pos > 0:
                self.cur_pos -= 1
            # Walk right.
            elif action == 1:
                self.cur_pos += 1
            # Set `done` flag when end of corridor (goal) reached.
            done = self.cur_pos >= self.end_pos
            # +1 when goal reached, otherwise -1.
            reward = 1.0 if done else -0.1
            return [self.cur_pos], reward, done, {}


    # Create an RLlib Trainer instance.
    trainer = PPOTrainer(
        config={
            # Env class to use (here: our gym.Env sub-class from above).
            "env": SimpleCorridor,
            # Config dict to be passed to our custom env's constructor.
            "env_config": {
                # Use corridor with 20 fields (including S and G).
                "corridor_length": 20
            },
            # Parallelize environment rollouts.
            "num_workers": 3,
        })

    # Train for n iterations and report results (mean episode rewards).
    # Since we have to move at least 19 times in the env to reach the goal and
    # each move gives us -0.1 reward (except the last move at the end: +1.0),
    # we can expect to reach an optimal episode reward of -0.1*18 + 1.0 = -0.8
    for i in range(5):
        results = trainer.train()
        print(f"Iter: {i}; avg. reward={results['episode_reward_mean']}")


After training, you may want to perform action computations (inference) in your environment.
Here is a minimal example on how to do this. Also
`check out our more detailed examples here <https://github.com/ray-project/ray/tree/master/rllib/examples/inference_and_serving>`_
(in particular for `normal models <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training.py>`_,
`LSTMs <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training_with_lstm.py>`_,
and `attention nets <https://github.com/ray-project/ray/blob/master/rllib/examples/inference_and_serving/policy_inference_after_training_with_attention.py>`_).

.. code-block:: python

    # Perform inference (action computations) based on given env observations.
    # Note that we are using a slightly different env here (len 10 instead of 20),
    # however, this should still work as the agent has (hopefully) learned
    # to "just always walk right!"
    env = SimpleCorridor({"corridor_length": 10})
    # Get the initial observation (should be: [0.0] for the starting position).
    obs = env.reset()
    done = False
    total_reward = 0.0
    # Play one episode.
    while not done:
        # Compute a single action, given the current observation
        # from the environment.
        action = trainer.compute_single_action(obs)
        # Apply the computed action in the environment.
        obs, reward, done, info = env.step(action)
        # Sum up rewards for reporting purposes.
        total_reward += reward
    # Report results.
    print(f"Played 1 episode; total-reward={total_reward}")


.. _`RLlib`: https://docs.ray.io/en/master/rllib/index.html


Ray Serve Quick Start
---------------------

.. image:: https://raw.githubusercontent.com/ray-project/ray/master/doc/source/serve/logo.svg
  :width: 400

`Ray Serve`_ is a scalable model-serving library built on Ray. It is:

- Framework Agnostic: Use the same toolkit to serve everything from deep
  learning models built with frameworks like PyTorch or Tensorflow & Keras
  to Scikit-Learn models or arbitrary business logic.
- Python First: Configure your model serving declaratively in pure Python,
  without needing YAMLs or JSON configs.
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

    import pickle
    import requests

    from sklearn.datasets import load_iris
    from sklearn.ensemble import GradientBoostingClassifier

    from ray import serve

    serve.start()

    # Train model.
    iris_dataset = load_iris()
    model = GradientBoostingClassifier()
    model.fit(iris_dataset["data"], iris_dataset["target"])

    @serve.deployment(route_prefix="/iris")
    class BoostingModel:
        def __init__(self, model):
            self.model = model
            self.label_list = iris_dataset["target_names"].tolist()

        async def __call__(self, request):
            payload = (await request.json())["vector"]
            print(f"Received flask request with data {payload}")

            prediction = self.model.predict([payload])[0]
            human_name = self.label_list[prediction]
            return {"result": human_name}


    # Deploy model.
    BoostingModel.deploy(model)

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
- `Exoshuffle: large-scale data shuffle in Ray`_ **(new)**
- `RLlib paper`_
- `RLlib flow paper`_
- `Tune paper`_

*Older documents:*

- `Ray paper`_
- `Ray HotOS paper`_

.. _`Documentation`: http://docs.ray.io/en/master/index.html
.. _`Tutorial`: https://github.com/ray-project/tutorial
.. _`Blog`: https://medium.com/distributed-computing-with-ray
.. _`Ray 1.0 Architecture whitepaper`: https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview
.. _`Exoshuffle: large-scale data shuffle in Ray`: https://arxiv.org/abs/2203.05072
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924
.. _`RLlib paper`: https://arxiv.org/abs/1712.09381
.. _`RLlib flow paper`: https://arxiv.org/abs/2011.12719
.. _`Tune paper`: https://arxiv.org/abs/1807.05118

Getting Involved
----------------

.. list-table::
   :widths: 25 50 25 25
   :header-rows: 1

   * - Platform
     - Purpose
     - Estimated Response Time
     - Support Level
   * - `Discourse Forum`_
     - For discussions about development and questions about usage.
     - < 1 day
     - Community
   * - `GitHub Issues`_
     - For reporting bugs and filing feature requests.
     - < 2 days
     - Ray OSS Team
   * - `Slack`_
     - For collaborating with other Ray users.
     - < 2 days
     - Community
   * - `StackOverflow`_
     - For asking questions about how to use Ray.
     - 3-5 days
     - Community
   * - `Meetup Group`_
     - For learning about Ray projects and best practices.
     - Monthly
     - Ray DevRel
   * - `Twitter`_
     - For staying up-to-date on new features.
     - Daily
     - Ray DevRel

.. _`Discourse Forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Meetup Group`: https://www.meetup.com/Bay-Area-Ray-Meetup/
.. _`Twitter`: https://twitter.com/raydistributed
.. _`Slack`: https://forms.gle/9TSdDYUgxYs8SA9e8

