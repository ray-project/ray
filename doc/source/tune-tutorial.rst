Tune Example Walkthrough
========================

This tutorial will walk you through the following process to setup a Tune experiment. Specifically, we'll leverage ASHA and Bayesian Optimization (via HyperOpt) without modifying your underlying code.

  1. Integrating Tune into your workflow
  2. Setting a stopping criteria
  3. Specifying a TrialScheduler
  4. Adding a SearchAlgorithm
  5. Getting the best model and analyzing results

We first run some imports:

.. code:: python

    # Original Code here: https://github.com/pytorch/examples/blob/master/mnist/main.py
    import numpy as np
    import torch
    import torch.optim as optim
    from torchvision import datasets

    from ray import tune
    from ray.tune import track
    from ray.tune.schedulers import AsyncHyperBandScheduler
    from ray.tune.examples.mnist_pytorch import get_data_loaders, Net, train, test

    datasets.MNIST("~/data", train=True, download=True)

Below, we have some boiler plate code for a PyTorch training function.

.. code:: python

    def train_mnist(config):
        model = Net(config)
        train_loader, test_loader = get_data_loaders()

        optimizer = optim.SGD(
            model.parameters(), lr=config["lr"], momentum=config["momentum"])

        for i in range(20):
            train(model, optimizer, train_loader)
            acc = test(model, test_loader)
            track.log(mean_accuracy=acc)
            if i % 5 == 0:
                torch.save(model, "./model.pth") # This saves the model to the trial directory

Notice that there's a couple helper functions in the above training script. You can take a look at these functions in the imported module `examples/mnist_pytorch <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch.py>`__; there's no black magic happening. For example, ``train`` is simply a for loop over the data loader.

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


Let's run 1 trial, randomly sampling from a uniform distribution for learning rate and momentum.

.. code:: python

    experiment_config = dict(
        name="train_mnist",
        stop={"mean_accuracy": 0.98}
    )

    search_space = {
        "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
        "momentum": tune.uniform(0.1, 0.9)
    }

    # Note: use `ray.init(redis_address=...)` to enable distributed execution
    analysis = tune.run(train_mnist, config=search_space, **experiment_config)

We can then plot the performance of this trial.

.. code:: python

    dfs = analysis.get_all_trial_dataframes()
    [d.mean_accuracy.plot() for d in dfs.values()]

Early Stopping with ASHA
~~~~~~~~~~~~~~~~~~~~~~~~

Now, let's integrate an early stopping algorithm to our search - ASHA. ASHA is a scalable algorithm for principled early stopping. How does it
work? On a high level, it terminates trials that are less promising and
allocates more time and resources to more promising trials. See `this blog post <ttps://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/>`__ for more details. Now, let's use this algorithm. With this, we can afford to **increase the search space by 5x**, by adjusting the parameter ``num_samples``.

.. code:: python

    from ray.tune.schedulers import ASHAScheduler

    analysis = tune.run(
        train_mnist,
        num_samples=30,
        scheduler=ASHAScheduler(metric="mean_accuracy", mode="max"),
        config=search_space,
        **experiment_config)

    # Plot by wall-clock time
    dfs = analysis.get_all_trial_dataframes()
    # This plots everything on the same plot
    ax = None
    for d in dfs.values():
        ax = d.plot("timestamp", "mean_accuracy", ax=ax, legend=False)

    # Plot by epoch
    ax = None
    for d in dfs.values():
        ax = d.mean_accuracy.plot(ax=ax, legend=False)


Search Algorithms in Tune
~~~~~~~~~~~~~~~~~~~~~~~~~

With Tune you can combine powerful Hyperparameter Search libraries such as `HyperOpt <https://github.com/hyperopt/hyperopt>`__ with state-of-the-art algorithms such as HyperBand without modifying any model training code. Tune allows you to use different search algorithms in combination with different trial schedulers.

.. code:: python

    from hyperopt import hp
    from ray.tune.suggest.hyperopt import HyperOptSearch

    space = {
        "lr": hp.loguniform("lr", 1e-10, 0.1),
        "momentum": hp.uniform("momentum", 0.1, 0.9),
    }

    hyperopt_search = HyperOptSearch(space, max_concurrent=2, reward_attr="mean_accuracy")

    analysis = tune.run(
        train_mnist,
        num_samples=10,
        search_alg=hyperopt_search
        **experiment_config)


Evaluate your model
~~~~~~~~~~~~~~~~~~~

You can evaluate best trained model using the Analysis object to retrieve the best model:

.. code:: python

    df = analysis.dataframe()

    logdir = analysis.get_best_logdir("mean_accuracy", mode="max")
    model = load_model(logdir + "/model.pth")

    res = model.evaluate(train_data, train_labels)
    print("Loss is {}".format(res[0]))
    print("Accuracy is {}".format(res[1]))
    predicted_label = model.predict(train_data)
    plot_data(train_data, predicted_label.argmax(1))

You can also use Tensorboard for visualizing results.

.. code:: bash

    $ tensorboard --logdir {logdir}


Feedback
--------

Please: fill out this form to provide feedback on this tutorial! https://goo.gl/forms/NVTFjUKFz4TH8kgK2
