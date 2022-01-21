.. _tune-pytorch-lightning:

Using PyTorch Lightning with Tune
=================================

PyTorch Lightning is a framework which brings structure into training PyTorch models. It
aims to avoid boilerplate code, so you don't have to write the same training
loops all over again when building a new model.

.. image:: /images/pytorch_lightning_full.png
  :align: center

The main abstraction of PyTorch Lightning is the ``LightningModule`` class, which
should be extended by your application. There is `a great post on how to transfer
your models from vanilla PyTorch to Lightning <https://towardsdatascience.com/from-pytorch-to-pytorch-lightning-a-gentle-introduction-b371b7caaf09>`_.

The class structure of PyTorch Lightning makes it very easy to define and tune model
parameters. This tutorial will show you how to use Tune to find the best set of
parameters for your application on the example of training a MNIST classifier. Notably,
the ``LightningModule`` does not have to be altered at all for this - so you can
use it plug and play for your existing models, assuming their parameters are configurable!

.. note::

    To run this example, you will need to install the following:

    .. code-block:: bash

        $ pip install "ray[tune]" torch torchvision pytorch-lightning

.. tip::

    If you want distributed PyTorch Lightning Training on Ray in addition to hyperparameter tuning with Tune,
    check out the `Ray Lightning Library <https://github.com/ray-project/ray_lightning>`_

.. contents::
    :local:
    :backlinks: none

PyTorch Lightning classifier for MNIST
--------------------------------------
Let's first start with the basic PyTorch Lightning implementation of an MNIST classifier.
This classifier does not include any tuning code at this point.

Our example builds on the MNIST example from the `blog post we talked about
earlier <https://towardsdatascience.com/from-pytorch-to-pytorch-lightning-a-gentle-introduction-b371b7caaf09>`_.

First, we run some imports:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __import_lightning_begin__
   :end-before: __import_lightning_end__

And then there is the Lightning model adapted from the blog post.
Note that we left out the test set validation and made the model parameters
configurable through a ``config`` dict that is passed on initialization.
Also, we specify a ``data_dir`` where the MNIST data will be stored. Note that
we use a ``FileLock`` for downloading data so that the dataset is only downloaded
once per node.
Lastly, we added a new metric, the validation accuracy, to the logs.

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __lightning_begin__
   :end-before: __lightning_end__

And that's it! You can now run ``train_mnist(config)`` to train the classifier, e.g.
like so:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __no_tune_train_begin__
   :end-before: __no_tune_train_end__

Tuning the model parameters
---------------------------
The parameters above should give you a good accuracy of over 90% already. However,
we might improve on this simply by changing some of the hyperparameters. For instance,
maybe we get an even higher accuracy if we used a larger batch size.

Instead of guessing the parameter values, let's use Tune to systematically try out
parameter combinations and find the best performing set.

First, we need some additional imports:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __import_tune_begin__
   :end-before: __import_tune_end__

Talking to Tune with a PyTorch Lightning callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PyTorch Lightning introduced `Callbacks <https://pytorch-lightning.readthedocs.io/en/latest/extensions/callbacks.html>`_
that can be used to plug custom functions into the training loop. This way the original
``LightningModule`` does not have to be altered at all. Also, we could use the same
callback for multiple modules.

Ray Tune comes with ready-to-use PyTorch Lightning callbacks. To report metrics
back to Tune after each validation epoch, we will use the ``TuneReportCallback``:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_train_begin__
   :end-before: __tune_train_end__
   :lines: 12-17
   :dedent: 12

This callback will take the ``val_loss`` and ``val_accuracy`` values
from the PyTorch Lightning trainer and report them to Tune as the ``loss``
and ``mean_accuracy``, respectively.

Adding the Tune training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Then we specify our training function. Note that we added the ``data_dir`` as a
parameter here to avoid
that each training run downloads the full MNIST dataset. Instead, we want to access
a shared data location.

We are also able to specify the number of epochs to train each model, and the number
of GPUs we want to use for training. We also create a TensorBoard logger that writes
logfiles directly into Tune's root trial directory - if we didn't do that PyTorch
Lightning would create subdirectories, and each trial would thus be shown twice in
TensorBoard, one time for Tune's logs, and another time for PyTorch Lightning's logs.

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_train_begin__
   :end-before: __tune_train_end__


Configuring the search space
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now we configure the parameter search space. We would like to choose between three
different layer and batch sizes. The learning rate should be sampled uniformly between
``0.0001`` and ``0.1``. The ``tune.loguniform()`` function is syntactic sugar to make
sampling between these different orders of magnitude easier, specifically
we are able to also sample small values.

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_asha_begin__
   :end-before: __tune_asha_end__
   :lines: 2-7
   :dedent: 4

Selecting a scheduler
~~~~~~~~~~~~~~~~~~~~~

In this example, we use an `Asynchronous Hyperband <https://blog.ml.cmu.edu/2018/12/12/massively-parallel-hyperparameter-optimization/>`_
scheduler. This scheduler decides at each iteration which trials are likely to perform
badly, and stops these trials. This way we don't waste any resources on bad hyperparameter
configurations.

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_asha_begin__
   :end-before: __tune_asha_end__
   :lines: 9-12
   :dedent: 4


Changing the CLI output
~~~~~~~~~~~~~~~~~~~~~~~

We instantiate a ``CLIReporter`` to specify which metrics we would like to see in our
output tables in the command line. This is optional, but can be used to make sure our
output tables only include information we would like to see.

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_asha_begin__
   :end-before: __tune_asha_end__
   :lines: 14-16
   :dedent: 4

Passing constants to the train function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``data_dir``, ``num_epochs`` and ``num_gpus`` we pass to the training function
are constants. To avoid including them as non-configurable parameters in the ``config``
specification, we can use ``tune.with_parameters`` to wrap around the training function.

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_asha_begin__
   :end-before: __tune_asha_end__
   :lines: 18-21
   :dedent: 4

Training with GPUs
~~~~~~~~~~~~~~~~~~
We can specify how many resources Tune should request for each trial.
This also includes GPUs.

PyTorch Lightning takes care of moving the training to the GPUs. We
already made sure that our code is compatible with that, so there's
nothing more to do here other than to specify the number of GPUs
we would like to use:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_asha_begin__
   :end-before: __tune_asha_end__
   :lines: 22
   :dedent: 4

You can also specify :doc:`fractional GPUs for Tune <../../ray-core/using-ray-with-gpus>`, allowing multiple trials to share GPUs
and thus increase concurrency under resource constraints. While the ``gpus_per_trial`` passed into
Tune is a decimal value, the ``gpus`` passed into the ``pl.Trainer`` should still be an integer.
Please note that if using fractional GPUs, it is the user's responsibility to
make sure multiple trials can share GPUs and there is enough memory to do so.
Ray does not automatically handle this for you.

If you want to use multiple GPUs per trial, you should check out the `Ray Lightning Library <https://github.com/ray-project/ray_lightning>`_.
This library makes it easy to run multiple concurrent trials with Ray Tune, with each trial also running
in a distributed fashion using Ray.

Putting it together
~~~~~~~~~~~~~~~~~~~

Lastly, we need to start Tune with ``tune.run()``.

The full code looks like this:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_asha_begin__
   :end-before: __tune_asha_end__


In the example above, Tune runs 10 trials with different hyperparameter configurations.
An example output could look like so:

.. code-block:: bash
  :emphasize-lines: 12

    +------------------------------+------------+-------+----------------+----------------+-------------+--------------+----------+-----------------+----------------------+
    | Trial name                   | status     | loc   |   layer_1_size |   layer_2_size |          lr |   batch_size |     loss |   mean_accuracy |   training_iteration |
    |------------------------------+------------+-------+----------------+----------------+-------------+--------------+----------+-----------------+----------------------|
    | train_mnist_tune_63ecc_00000 | TERMINATED |       |            128 |             64 | 0.00121197  |          128 | 0.120173 |       0.972461  |                   10 |
    | train_mnist_tune_63ecc_00001 | TERMINATED |       |             64 |            128 | 0.0301395   |          128 | 0.454836 |       0.868164  |                    4 |
    | train_mnist_tune_63ecc_00002 | TERMINATED |       |             64 |            128 | 0.0432097   |          128 | 0.718396 |       0.718359  |                    1 |
    | train_mnist_tune_63ecc_00003 | TERMINATED |       |             32 |            128 | 0.000294669 |           32 | 0.111475 |       0.965764  |                   10 |
    | train_mnist_tune_63ecc_00004 | TERMINATED |       |             32 |            256 | 0.000386664 |           64 | 0.133538 |       0.960839  |                    8 |
    | train_mnist_tune_63ecc_00005 | TERMINATED |       |            128 |            128 | 0.0837395   |           32 | 2.32628  |       0.0991242 |                    1 |
    | train_mnist_tune_63ecc_00006 | TERMINATED |       |             64 |            128 | 0.000158761 |          128 | 0.134595 |       0.959766  |                   10 |
    | train_mnist_tune_63ecc_00007 | TERMINATED |       |             64 |             64 | 0.000672126 |           64 | 0.118182 |       0.972903  |                   10 |
    | train_mnist_tune_63ecc_00008 | TERMINATED |       |            128 |             64 | 0.000502428 |           32 | 0.11082  |       0.975518  |                   10 |
    | train_mnist_tune_63ecc_00009 | TERMINATED |       |             64 |            256 | 0.00112894  |           32 | 0.13472  |       0.971935  |                    8 |
    +------------------------------+------------+-------+----------------+----------------+-------------+--------------+----------+-----------------+----------------------+

As you can see in the ``training_iteration`` column, trials with a high loss
(and low accuracy) have been terminated early. The best performing trial used
``layer_1_size=128``, ``layer_2_size=64``, ``lr=0.000502428`` and
``batch_size=32``.

Using Population Based Training to find the best parameters
-----------------------------------------------------------
The ``ASHAScheduler`` terminates those trials early that show bad performance.
Sometimes, this stops trials that would get better after more training steps,
and which might eventually even show better performance than other configurations.

Another popular method for hyperparameter tuning, called
`Population Based Training <https://deepmind.com/blog/article/population-based-training-neural-networks>`_,
instead perturbs hyperparameters during the training run. Tune implements PBT, and
we only need to make some slight adjustments to our code.

Adding checkpoints to the PyTorch Lightning module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, we need to introduce
another callback to save model checkpoints. Since Tune requires a call to
``tune.report()`` after creating a new checkpoint to register it, we will use
a combined reporting and checkpointing callback:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_train_checkpoint_begin__
   :end-before: __tune_train_checkpoint_end__
   :lines: 15-21
   :dedent: 12

The ``checkpoint`` value is the name of the checkpoint file within the
checkpoint directory.

We also include checkpoint loading in our training function:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_train_checkpoint_begin__
   :end-before: __tune_train_checkpoint_end__


Configuring and running Population Based Training
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We need to call Tune slightly differently:

.. literalinclude:: /../../python/ray/tune/examples/mnist_pytorch_lightning.py
   :language: python
   :start-after: __tune_pbt_begin__
   :end-before: __tune_pbt_end__

Instead of passing tune parameters to the ``config`` dict, we start
with fixed values, though we are also able to sample some of them, like the
layer sizes. Additionally, we have to tell PBT how to perturb the hyperparameters.
Note that the layer sizes are not tuned right here. This is because we cannot simply
change layer sizes during a training run - which is what would happen in PBT.

An example output could look like this:

.. code-block:: bash

    +-----------------------------------------+------------+-------+----------------+----------------+-----------+--------------+-----------+-----------------+----------------------+
    | Trial name                              | status     | loc   |   layer_1_size |   layer_2_size |        lr |   batch_size |      loss |   mean_accuracy |   training_iteration |
    |-----------------------------------------+------------+-------+----------------+----------------+-----------+--------------+-----------+-----------------+----------------------|
    | train_mnist_tune_checkpoint_85489_00000 | TERMINATED |       |            128 |            128 | 0.001     |           64 | 0.108734  |        0.973101 |                   10 |
    | train_mnist_tune_checkpoint_85489_00001 | TERMINATED |       |            128 |            128 | 0.001     |           64 | 0.093577  |        0.978639 |                   10 |
    | train_mnist_tune_checkpoint_85489_00002 | TERMINATED |       |            128 |            256 | 0.0008    |           32 | 0.0922348 |        0.979299 |                   10 |
    | train_mnist_tune_checkpoint_85489_00003 | TERMINATED |       |             64 |            256 | 0.001     |           64 | 0.124648  |        0.973892 |                   10 |
    | train_mnist_tune_checkpoint_85489_00004 | TERMINATED |       |            128 |             64 | 0.001     |           64 | 0.101717  |        0.975079 |                   10 |
    | train_mnist_tune_checkpoint_85489_00005 | TERMINATED |       |             64 |             64 | 0.001     |           64 | 0.121467  |        0.969146 |                   10 |
    | train_mnist_tune_checkpoint_85489_00006 | TERMINATED |       |            128 |            256 | 0.00064   |           32 | 0.053446  |        0.987062 |                   10 |
    | train_mnist_tune_checkpoint_85489_00007 | TERMINATED |       |            128 |            256 | 0.001     |           64 | 0.129804  |        0.973497 |                   10 |
    | train_mnist_tune_checkpoint_85489_00008 | TERMINATED |       |             64 |            256 | 0.0285125 |          128 | 0.363236  |        0.913867 |                   10 |
    | train_mnist_tune_checkpoint_85489_00009 | TERMINATED |       |             32 |            256 | 0.001     |           64 | 0.150946  |        0.964201 |                   10 |
    +-----------------------------------------+------------+-------+----------------+----------------+-----------+--------------+-----------+-----------------+----------------------+

As you can see, each sample ran the full number of 10 iterations.
All trials ended with quite good parameter combinations and showed relatively good performances.
In some runs, the parameters have been perturbed. And the best configuration even reached a
mean validation accuracy of ``0.987062``!

In summary, PyTorch Lightning Modules are easy to extend to use with Tune. It just took
us importing one or two callbacks and a small wrapper function to get great performing
parameter configurations.
