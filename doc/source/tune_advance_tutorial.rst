Tune Advance tutorial
=====================

In this page, we will explore some advanced functions in Tune with more examples.

Current available tutorials:

.. contents::
    :local:
    :backlinks: none

A native example of Trainable
----------------------------------------------
As mentioned in `Tune User Guide <tune-usage.html#Tune Training API>`_, Training can be done
with either the `Trainable <tune-usage.html#trainable-api>`__ Trainable **Class API** or
**function-based API**. Comparably, ``Trainable`` supports stateful Actor and checkpoint/restore,
and is preferable for advanced algorithms. For the subclass of ``ray.tune.Trainable``, Tune will
convert this class into a Ray actor, which runs on a separate process on a worker.

A naive example for ``Trainable`` is a simple number guesser:

.. code-block:: python
    import ray
    from ray import tune
    from ray.tune import Trainable

    class Example(Trainable):
        def _setup(self, config):
            self.config = config
            self.password = 1024
            pass

        def _train(self):
            result_dict = {"diff": abs(self.config['guess'] - self.password)}
            return result_dict

    ray.init()
    analysis = tune.run(
        Example,
        stop={
            "training_iteration": 1,
        },
        num_samples=10,
        config={
            "guess": tune.randint(1, 10000)
        })

    print('best config: ', analysis.get_best_config(metric="diff", mode="min"))

The program randomly picks 10 number from [1, 10000) and checks which is closer to the password.
As a minimized example, we use the BasicVariantGenerator as search algorithm and it cannot further
mutate the (hyper)parameter in config across the experiments. We only implemented ``_setup`` and ``_train``
methods, usually users also need to implement ``_save``, and ``_restore`` for checkpoint and fault tolerance.
``_setup`` runs once for custom initialization. ``_train`` execute one logical iteration of training. As a
rule of thumb, the execution time of one train call should be large enough to avoid overheads (i.e. more
than a few seconds), but short enough to report progress periodically (i.e. at most a few minutes).

Next we use a more complete example, training a Pytorch model with Trainable and PBT.

Trainable with Population Based Training (PBT)
----------------------------------------------

Tune includes a distributed implementation of `Population Based Training (PBT) <https://deepmind.com/blog/population-based-training-neural-networks>`__ as
a scheduler `PopulationBasedTraining <tune-schedulers.html#Population Based Training (PBT)>`__ .
PBT starts by training many neural networks in parallel with random hyperparameters. But instead of the
networks training independently, it uses information from the rest of the population to refine the
hyperparameters and direct computational resources to models which show promise. This takes its inspiration
from genetic algorithms where each member of the population, known as a worker, can exploit information
from the remainder of the population. For example, a worker might copy the model parameters from a better
performing worker. It can also explore new hyperparameters by changing the current values randomly.

As the training of the population of neural networks progresses, this process of exploiting and exploring
is performed periodically, ensuring that all the workers in the population have a good base level of performance
and also that new hyperparameters are consistently explored.  This means that PBT can quickly exploit good
hyperparameters, can dedicate more training time to promising models and, crucially, can adapt the hyperparameter
values throughout training, leading to automatic learning of the best configurations.

First we define a Trainable that wraps a ConvNet model.

.. literalinclude:: ../../python/ray/tune/example/pbt_pytorch_trainable.py
   :language: python
   :start-after: __trainable_begin__
   :end-before: __trainable_end__

The example reuse some of the functions in mnist_pytorch, and is a good demo for how to decouple
the tuning function and original training code.

Here we also overrides ``reset_config``. This method is optional, but can be implemented to speed
up algorithms such as PBT, and to allow performance optimizations such as running experiments
with reuse_actors=True.

Then we define a PBT scheduler

.. literalinclude:: ../../python/ray/tune/example/pbt_pytorch_trainable.py
   :language: python
   :start-after: __pbt_begin__
   :end-before: __pbt_end__

Some of the most important parameters are:
``hyperparam_mutations`` and ``custom_explore_fn`` are used to mutate the hyperparameters.
``hyperparam_mutations`` is a dictionary where each key/value pair specifies the candidates
or function for a hyperparameter. custom_explore_fn is applied after built-in perturbations
from hyperparam_mutations are applied, and should return config updated as needed.

Now we can kick off the tuning process by invoking tune.run:

.. literalinclude:: ../../python/ray/tune/example/pbt_pytorch_trainable.py
   :language: python
   :start-after: __tune_begin__
   :end-before: __tune_end__

During the training, we can constantly check the status of the models from console log:


The best model gets an accuracy of , and the result.json stores all the config information.


