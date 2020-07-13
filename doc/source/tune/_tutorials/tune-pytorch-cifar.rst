.. _tune-pytorch-cifar:

How to use Tune with PyTorch
============================

In this walkthrough, we will show you how to integrate Tune into your PyTorch
training workflow. We will follow `this tutorial from the PyTorch documentation
<https://pytorch.org/tutorials/beginner/blitz/cifar10_tutorial.html>`_ for training
a CIFAR10 image classifier.

.. image:: /images/pytorch_logo.png
  :align: center

Hyperparameter tuning can make the difference between an average model and a highly
accurate one. Often simple things like choosing a different learning rate or changing
a network layer size can have a dramatic impact on your model performance. Fortunately,
Tune makes exploring these optimal parameter combinations easy - and works nicely
together with PyTorch.

As you will see, we only need to add some slight modifications. In particular, we
need to

1. wrap data loading and training in functions,
2. make some network parameters configurable,
3. add checkpointing (optional),
4. and define the search space for the model tuning

.. note::

    To run this example, you will need to install the following:

    .. code-block:: bash

        $ pip install ray torch torchvision

.. contents::
    :local:
    :backlinks: none

Setup / Imports
---------------
Let's start with the imports:

.. literalinclude:: /../../python/ray/tune/examples/cifar10_pytorch.py
   :language: python
   :start-after: __import_begin__
   :end-before: __import_end__

Most of the imports are needed for building the PyTorch model. Only the last three
imports are for Ray Tune.

Data loaders
------------
We wrap the data loaders in their own function and pass a global data directory.
This way we can share a data directory between different trials.

.. literalinclude:: /../../python/ray/tune/examples/cifar10_pytorch.py
   :language: python
   :start-after: __load_data_begin__
   :end-before: __load_data_end__

Configurable neural network
---------------------------
We can only tune those parameters that are configurable. In this example, we can specify
the layer sizes of the fully connected layers:

.. literalinclude:: /../../python/ray/tune/examples/cifar10_pytorch.py
   :language: python
   :start-after: __net_begin__
   :end-before: __net_end__

The train function
------------------
Now it gets interesting, because we introduce some changes to the example `from the PyTorch
documentation <https://pytorch.org/tutorials/beginner/blitz/cifar10_tutorial.html>`_.

We wrap the training script in a function ``train_cifar(config, checkpoint=None)``. As you
can guess, the ``config`` parameter will receive the hyperparameters we would like to
train with. The ``checkpoint`` parameter is used to restore checkpoints.

.. code-block:: python

    net = Net(config["l1"], config["l2"])

    if checkpoint:
        net.load_state_dict(torch.load(checkpoint))

The learning rate of the optimizer is made configurable, too:

.. code-block:: python

    optimizer = optim.SGD(net.parameters(), lr=config["lr"], momentum=0.9)

We also split the training data into a training and validation subset. We thus train on
80% of the data and calculate the validation loss on the remaining 20%. The batch sizes
with which we iterate through the training and test sets are configurable as well.

Adding (multi) GPU support with DataParallel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Image classification benefits largely from GPUs. Luckily, we can continue to use
PyTorch's abstractions in Ray Tune. Thus, we can wrap our model in ``nn.DataParallel``
to support data parallel training on multiple GPUs:

.. code-block:: python

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda:0"
        if torch.cuda.device_count() > 1:
            net = nn.DataParallel(net)
    net.to(device)

By using a ``device`` variable we make sure that training also works when we have
no GPUs available. PyTorch requires us to send our data to the GPU memory explicitly,
like this:

.. code-block:: python

    for i, data in enumerate(trainloader, 0):
        inputs, labels = data
        inputs, labels = inputs.to(device), labels.to(device)

The code now supports training on CPUs, on a single GPU, and on multiple GPUs. Notably, Ray
also supports :doc:`fractional GPUs </using-ray-with-gpus>`
so we can share GPUs among trials, as long as the model still fits on the GPU memory. We'll come back
to that later.

Communicating with Ray Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most interesting part is the communication with Tune:

.. code-block:: python

    checkpoint_dir = tune.make_checkpoint_dir(epoch)
    path = os.path.join(checkpoint_dir, "checkpoint")
    torch.save((net.state_dict(), optimizer.state_dict()), path)
    tune.save_checkpoint(path)

    tune.report(loss=(val_loss / val_steps), accuracy=correct / total)

Here we first save a checkpoint and then report some metrics back to Tune. Specifically,
we send the validation loss and accuracy back to Tune. Tune can then use these metrics
to decide which hyperparameter configuration lead to the best results. These metrics
can also be used to stop bad performing trials early in order to avoid wasting
resources on those trials.

The checkpoint saving is optional, however, it is necessary if we wanted to use advanced
schedulers like `Population Based Training <https://docs.ray.io/en/master/tune/tutorials/tune-advanced-tutorial.html>`_.
Also, by saving the checkpoint we can later load the trained models and validate them
on a test set.

Full training function
~~~~~~~~~~~~~~~~~~~~~~

The full code example looks like this:

.. literalinclude:: /../../python/ray/tune/examples/cifar10_pytorch.py
   :language: python
   :start-after: __train_begin__
   :end-before: __train_end__
   :emphasize-lines: 2,4-9,12,14-18,28,33,43,70,81-84,86

As you can see, most of the code is adapted directly from the example.

Test set accuracy
-----------------
Commonly the performance of a machine learning model is tested on a hold-out test
set with data that has not been used for training the model. We also wrap this in a
function:

.. literalinclude:: /../../python/ray/tune/examples/cifar10_pytorch.py
   :language: python
   :start-after: __test_acc_begin__
   :end-before: __test_acc_end__

As you can see, the function also expects a ``device`` parameter, so we can do the
test set validation on a GPU.

Configuring the search space
----------------------------
Lastly, we need to define Tune's search space. Here is an example:

.. code-block:: python

    config = {
        "l1": tune.sample_from(lambda _: 2**np.random.randint(2, 9)),
        "l2": tune.sample_from(lambda _: 2**np.random.randint(2, 9)),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([2, 4, 8, 16]),
        "data_dir": data_dir
    }

The ``tune.sample_from()`` function makes it possible to define your own sample
methods to obtain hyperparameters. In this example, the ``l1`` and ``l2`` parameters
should be powers of 2 between 4 and 256, so either 4, 8, 16, 32, 64, 128, or 256.
The ``lr`` (learning rate) should be uniformly sampled between 0.0001 and 0.1. Lastly,
the batch size is a choice between 2, 4, 8, and 16.

At each trial, Tune will now randomly sample a combination of parameters from these
search spaces. It will then train a number of models in parallel and find the best
performing one among these. We also use the ``ASHAScheduler`` which will terminate bad
performing trials early.

We wrap the ``train_cifar`` function with ``functools.partial`` to set the constant
``data_dir`` parameter. We can also tell Ray Tune what resources should be
available for each trial:

.. code-block:: python

    gpus_per_trial = 2
    # ...
    result = tune.run(
        partial(train_cifar, data_dir=data_dir),
        resources_per_trial={"cpu": 8, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        checkpoint_at_end=True)

You can specify the number of CPUs, which are then available e.g.
to increase the ``num_workers`` of the PyTorch ``DataLoader`` instances. The selected
number of GPUs are made visible to PyTorch in each trial. Trials do not have access to
GPUs that haven't been requested for them - so you don't have to care about two trials
using the same set of resources.

Here we can also specify fractional GPUs, so something like ``gpus_per_trial=0.5`` is
completely valid. The trials will then share GPUs among each other.
You just have to make sure that the models still fit in the GPU memory.

After training the models, we will find the best performing one and load the trained
network from the checkpoint file. We then obtain the test set accuracy and report
everything by printing.

The full main function looks like this:

.. literalinclude:: /../../python/ray/tune/examples/cifar10_pytorch.py
   :language: python
   :start-after: __main_begin__
   :end-before: __main_end__

If you run the code, an example output could look like this:

.. code-block::
  :emphasize-lines: 7

    Number of trials: 10 (10 TERMINATED)
    +-------------------------+------------+-------+------+------+-------------+--------------+---------+------------+----------------------+
    | Trial name              | status     | loc   |   l1 |   l2 |          lr |   batch_size |    loss |   accuracy |   training_iteration |
    |-------------------------+------------+-------+------+------+-------------+--------------+---------+------------+----------------------|
    | train_cifar_87d1f_00000 | TERMINATED |       |   64 |    4 | 0.00011629  |            2 | 1.87273 |     0.244  |                    2 |
    | train_cifar_87d1f_00001 | TERMINATED |       |   32 |   64 | 0.000339763 |            8 | 1.23603 |     0.567  |                    8 |
    | train_cifar_87d1f_00002 | TERMINATED |       |    8 |   16 | 0.00276249  |           16 | 1.1815  |     0.5836 |                   10 |
    | train_cifar_87d1f_00003 | TERMINATED |       |    4 |   64 | 0.000648721 |            4 | 1.31131 |     0.5224 |                    8 |
    | train_cifar_87d1f_00004 | TERMINATED |       |   32 |   16 | 0.000340753 |            8 | 1.26454 |     0.5444 |                    8 |
    | train_cifar_87d1f_00005 | TERMINATED |       |    8 |    4 | 0.000699775 |            8 | 1.99594 |     0.1983 |                    2 |
    | train_cifar_87d1f_00006 | TERMINATED |       |  256 |    8 | 0.0839654   |           16 | 2.3119  |     0.0993 |                    1 |
    | train_cifar_87d1f_00007 | TERMINATED |       |   16 |  128 | 0.0758154   |           16 | 2.33575 |     0.1327 |                    1 |
    | train_cifar_87d1f_00008 | TERMINATED |       |   16 |    8 | 0.0763312   |           16 | 2.31129 |     0.1042 |                    4 |
    | train_cifar_87d1f_00009 | TERMINATED |       |  128 |   16 | 0.000124903 |            4 | 2.26917 |     0.1945 |                    1 |
    +-------------------------+------------+-------+------+------+-------------+--------------+---------+------------+----------------------+


    Best trial config: {'l1': 8, 'l2': 16, 'lr': 0.0027624906698231976, 'batch_size': 16, 'data_dir': '...'}
    Best trial final validation loss: 1.1815014744281769
    Best trial final validation accuracy: 0.5836
    Best trial test set accuracy: 0.5806

As you can see, most trials have been stopped early in order to avoid wasting resources.
The best performing trial achieved a validation accuracy of about 58%, which could
be confirmed on the test set.

So that's it! You can now tune the parameters of your PyTorch models.

If you consider switching to PyTorch Lightning to get rid of some of your boilerplate
training code, please know that we also have a walkthrough on :doc:`how to use Tune with
PyTorch Lightning models <tune-pytorch-lightning>`.