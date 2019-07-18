Tune Walkthrough
================

Tuning hyperparameters is often the most expensive part of the machine
learning workflow. Tune is built to address this, demonstrating an
efficient and scalable solution for this pain point.

**Code**: https://github.com/ray-project/ray/tree/master/python/ray/tune

**Examples**:
https://github.com/ray-project/ray/tree/master/python/ray/tune/examples

**Documentation**: http://ray.readthedocs.io/en/latest/tune.html

**Mailing List** https://groups.google.com/forum/#!forum/ray-dev

This tutorial will walk you through the following process:

1. Integrating Tune into your workflow
2. Setting a stopping criteria
3. Getting the best model and analyzing results

.. code:: ipython3

    from keras.models import Sequential
    from keras.layers import Dense
    from keras.optimizers import SGD, Adam
    from keras.callbacks import ModelCheckpoint

    from ray import tune
    from ray.tune.integration.keras import TuneReporterCallback
    from ray.tune.examples.utils import get_iris_data

    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    plt.style.use('ggplot')
    %matplotlib inline

Visualize your data
-------------------

Let's first take a look at the distribution of the dataset.

.. code:: ipython3

    from sklearn.datasets import load_iris

    iris = load_iris()
    true_data = iris['data']
    true_label = iris['target']
    names = iris['target_names']
    feature_names = iris['feature_names']

    def plot_data(X, y):
        # Visualize the data sets
        plt.figure(figsize=(16, 6))
        plt.subplot(1, 2, 1)
        for target, target_name in enumerate(names):
            X_plot = X[y == target]
            plt.plot(X_plot[:, 0], X_plot[:, 1], linestyle='none', marker='o', label=target_name)
        plt.xlabel(feature_names[0])
        plt.ylabel(feature_names[1])
        plt.axis('equal')
        plt.legend();

        plt.subplot(1, 2, 2)
        for target, target_name in enumerate(names):
            X_plot = X[y == target]
            plt.plot(X_plot[:, 2], X_plot[:, 3], linestyle='none', marker='o', label=target_name)
        plt.xlabel(feature_names[2])
        plt.ylabel(feature_names[3])
        plt.axis('equal')
        plt.legend();

    plot_data(true_data, true_label)

Now, let's define a function that will train a model to classify this
dataset.

.. code:: ipython3

    def train_on_iris():
        train_x, train_y, test_x, test_y = get_iris_data()
        model = Sequential()

        model.add(Dense(2, input_shape=(4,), activation='relu', name='fc1'))
        model.add(Dense(2, activation='relu', name='fc2'))
        model.add(Dense(3, activation='softmax', name='output'))
        optimizer = SGD(lr=0.1)
        model.compile(optimizer, loss='categorical_crossentropy', metrics=['accuracy'])
        # This saves the top model
        checkpoint_callback = ModelCheckpoint("model.h5", monitor='val_loss', save_best_only=True, period=3)


        # Train the model
        model.fit(
            train_x, train_y,
            validation_data=(test_x, test_y),
            verbose=0, batch_size=5, epochs=50, callbacks=[checkpoint_callback])
        return model

.. code:: ipython3

    model = train_on_iris()
    train_x, train_y, test_x, test_y = get_iris_data()
    model.evaluate(train_x, train_y)

Integrate with Tune
-------------------

Now, let's use Tune to optimize a model that learns to classify Iris.
This will take three steps:

1. Designate the hyperparameter space.

2. Set a callback to report results back to Tune
3. Increase the number of samples

.. code:: ipython3

    def tune_iris(config):
        train_x, train_y, test_x, test_y = get_iris_data()
        model = Sequential()

        model.add(Dense(config["dense_1"], input_shape=(4,), activation='relu', name='fc1'))
        model.add(Dense(config["dense_2"], activation='relu', name='fc2'))
        model.add(Dense(3, activation='softmax', name='output'))
        optimizer = SGD(lr=config["lr"])
        model.compile(optimizer, loss='categorical_crossentropy', metrics=['accuracy'])
        checkpoint_callback = ModelCheckpoint("model.h5", monitor='val_loss', save_best_only=True, period=3)


        # Train the model
        model.fit(
            train_x, train_y,
            validation_data=(test_x, test_y),
            verbose=0,
            batch_size=5,
            epochs=50,
            callbacks=[checkpoint_callback, TuneReporterCallback(freq="epoch")])


    results = tune.run(
        tune_iris,
        config={"lr": 0.1, "dense_1": 1, "dense_2": 0.1},
        num_samples=1,
        return_trials=False)

    assert len(results.trials) == 10

Evaluate best trained model
---------------------------

.. code:: ipython3

    df = results.dataframe()

    logdir = results.get_best_logdir("keras_info:val_loss", mode="min")

    # import keras.models
    from keras.models import load_model
    model = load_model(logdir + "/model.h5")

    train_data, train_labels, _, _ = get_iris_data()
    plot_data(train_data, train_labels.argmax(1))

.. code:: ipython3

    res = model.evaluate(train_data, train_labels)
    print("Loss is {}".format(res[0]))
    print("Accuracy is {}".format(res[1]))
    predicted_label = model.predict(train_data)
    plot_data(train_data, predicted_label.argmax(1))

Use Tensorboard for results
---------------------------

.. code:: ipython3

    ! ls {logdir}

.. code:: ipython3

    ! tensorboard --logdir {logdir}

In this tutorial, we'll show you how to use state-of-the-art hyperparameter tuning with Tune and PyTorch.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Specifically, we'll leverage ASHA and Bayesian Optimization (via
HyperOpt) without modifying your underlying code.

Tune is a scalable framework for model training and hyperparameter
search with a focus on deep learning and deep reinforcement learning.

-  **Code**:
   https://github.com/ray-project/ray/tree/master/python/ray/tune
-  **Examples**:
   https://github.com/ray-project/ray/tree/master/python/ray/tune/examples
-  **Documentation**: http://ray.readthedocs.io/en/latest/tune.html
-  **Mailing List** https://groups.google.com/forum/#!forum/ray-dev

Exercise 1: PyTorch Boilerplate Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the below cells to see what you would do with Tune without any
additional optimization techniques. You'll see that integrating Tune
with PyTorch **requires 1 line of code**!

.. code:: ipython3

    # This is some basic imports.
    # Original Code here:
    # https://github.com/pytorch/examples/blob/master/mnist/main.py
    import numpy as np
    import torch
    import torch.optim as optim
    from torchvision import datasets
    from helper import train, test, ConvNet, get_data_loaders

    from ray import tune
    from ray.tune import track
    from ray.tune.schedulers import AsyncHyperBandScheduler

    %matplotlib inline
    import matplotlib.style as style
    style.use("ggplot")

    datasets.MNIST("~/data", train=True, download=True)

Below, we have some boiler plate code for a PyTorch training function.
You can take a look at these functions in ``helper.py``; there's no
black magic happening. For example, ``train`` is simply a for loop over
the data loader.

.. code:: python

        def train(model, optimizer, train_loader):
            model.train()
            for batch_idx, (data, target) in enumerate(train_loader):
                if batch_idx * len(data) > EPOCH_SIZE:
                    return
                optimizer.zero_grad()
                output = model(data)
                loss = F.nll_loss(output, target)
                loss.backward()
                optimizer.step()

**TODO**: Add ``track.log(mean_accuracy=acc)`` within the training loop.
``tune.track`` allows Tune to keep track of current results.

.. code:: ipython3

    def train_mnist(config):
        model = ConvNet(config)
        train_loader, test_loader = get_data_loaders()

        optimizer = optim.SGD(
            model.parameters(), lr=config["lr"], momentum=config["momentum"])

        for i in range(20):
            train(model, optimizer, train_loader)
            acc = test(model, test_loader)
            # TODO: Add track.log(mean_accuracy=acc) here
            if i % 5 == 0:
                torch.save(model, "./model.pth") # This saves the model to the trial directory

Let's run 1 trial, randomly sampling from a uniform distribution for learning rate and momentum.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Run the below cell to run Tune.

.. code:: ipython3

    experiment_config = dict(
        name="train_mnist",
        stop={"mean_accuracy": 0.98},
        return_trials=False
    )

    search_space = {
        "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
        "momentum": tune.uniform(0.1, 0.9)
    }

    # Note: use `ray.init(redis_address=...)` to enable distributed execution
    analysis = tune.run(train_mnist, config=search_space, **experiment_config)

Plot the performance of this trial.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: ipython3

    dfs = analysis.get_all_trial_dataframes()
    [d.mean_accuracy.plot() for d in dfs.values()]

Exercise 2: Early Stopping with ASHA
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ASHA is a scalable algorithm for principled early stopping. How does it
work? On a high level, it terminates trials that are less promising and
allocates more time and resources to more promising trials.

::

    The successive halving algorithm begins with all candidate configurations in the base rung and proceeds as follows:

        1. Uniformly allocate a budget to a set of candidate hyperparameter configurations in a given rung.
        2. Evaluate the performance of all candidate configurations.
        3. Promote the top half of candidate configurations to the next rung.
        4. Double the budget per configuration for the next rung and repeat until one configurations remains.


A textual representation:

::

           | Configurations | Epochs per
           | Remaining      | Configuration
    ---------------------------------------
    Rung 1 | 27             | 1
    Rung 2 | 9              | 3
    Rung 3 | 3              | 9
    Rung 4 | 1              | 27

(from
https://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/)

Now, let's integrate this with a PyTorch codebase.

**TODO**: Set up ASHA.

1) Create an Asynchronous HyperBand Scheduler (ASHA). \`\`\`python from
   ray.tune.schedulers import ASHAScheduler

custom\_scheduler = ASHAScheduler( reward\_attr='mean\_accuracy',
grace\_period=1, ) \`\`\`

*Note: Read the documentation on this step at
https://ray.readthedocs.io/en/latest/tune-schedulers.html#asynchronous-hyperband
or call ``help(tune.schedulers.AsyncHyperBandScheduler)`` to learn more
about the Asynchronous Hyperband Scheduler*

2) With this, we can afford to **increase the search space by 5x**. To
   do this, set the parameter ``num_samples``. For example,

.. code:: python

    tune.run(... num_samples=30)

.. code:: ipython3

    from ray.tune.schedulers import ASHAScheduler

    custom_scheduler = "FIX ME"

    analysis = tune.run(
        train_mnist,
        num_samples="FIX ME",
        scheduler=custom_scheduler,
        config=search_space,
        **experiment_config)

.. code:: ipython3

    # Plot by wall-clock time

    dfs = analysis.get_all_trial_dataframes()
    # This plots everything on the same plot
    ax = None
    for d in dfs.values():
        ax = d.plot("timestamp", "mean_accuracy", ax=ax, legend=False)

.. code:: ipython3

    # Plot by epoch
    ax = None
    for d in dfs.values():
        ax = d.mean_accuracy.plot(ax=ax, legend=False)

Exercise 3: Search Algorithms in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Tune you can combine powerful Hyperparameter Search libraries such
as HyperOpt (https://github.com/hyperopt/hyperopt) with state-of-the-art
algorithms such as HyperBand without modifying any model training code.
Tune allows you to use different search algorithms in combination with
different trial schedulers.

The documentation to doing this is here:
https://ray.readthedocs.io/en/latest/tune-searchalg.html#hyperopt-search-tree-structured-parzen-estimators

Currently, Tune offers the following search algorithms (and library
integrations):

-  Grid Search and Random Search
-  BayesOpt
-  HyperOpt
-  SigOpt
-  Nevergrad
-  Scikit-Optimize
-  Ax

Check out more at
https://ray.readthedocs.io/en/latest/tune-searchalg.html

**TODO:** Plug in ``HyperOptSearch`` into ``tune.run`` and enforce that
only 2 trials can run concurrently, like this -

.. code:: python

        hyperopt_search = HyperOptSearch(space, max_concurrent=2, reward_attr="mean_accuracy")

.. code:: ipython3

    from hyperopt import hp
    from ray.tune.suggest.hyperopt import HyperOptSearch

    space = {
        "lr": hp.loguniform("lr", 1e-10, 0.1),
        "momentum": hp.uniform("momentum", 0.1, 0.9),
    }

    hyperopt_search = "FIX ME"  # TODO: Change this

    analysis = tune.run(
        train_mnist,
        num_samples=10,
        search_alg="FIX ME",  #  TODO: Change this
        **experiment_config)

Please: fill out this form to provide feedback on this tutorial!
================================================================

https://goo.gl/forms/NVTFjUKFz4TH8kgK2
