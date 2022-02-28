.. _sgd-migration:

Migrating from Ray SGD to Ray Train
===================================

In Ray 1.7, we are rolling out a new and more streamlined version of Ray SGD.
As of Ray 1.8, the new version of Ray SGD has been rebranded as Ray Train.
Ray Train focuses on usability and composability - it has a much simpler API, has support for more deep learning backends, integrates better with other libraries in the Ray ecosystem, and will continue to be actively developed with more features.

This guide will help you easily migrate existing code from Ray SGD v1 to Ray Train. If you are new to Ray SGD as a whole, you should get started with :ref:`Ray Train directly <train-docs>`.

For a full list of features that Ray Train provides, please check out the :ref:`user guide<train-user-guide>`.

.. note:: If there are any issues or anything missing with this guide or any feedback on Ray Train overall, please file a `Github issue on the Ray repo  <https://github.com/ray-project/ray/issues>`_!

What are the API differences?
-----------------------------

There are 3 primary API differences between Ray SGD v1 and Ray Train.

1. There is a single ``Trainer`` interface for all backends (torch, tensorflow, horovod), and the backend is simply specified via an argument: ``Trainer(backend="torch")``\ , ``Trainer(backend="horovod")``\ , etc. Any features that we add to Ray SGD will be supported for all backends, and there won't be any API divergence like there was with a separate ``TorchTrainer`` and ``TFTrainer``.
2. The ``TrainingOperator`` and creator functions are replaced by a more natural user-defined training function. You no longer have to make your training logic fit into a restrictive interface. In Ray SGD v2, you simply have to provide a training function that describes the full logic for your training execution and this will be distributed by Ray Train.

    .. code-block:: python

        from torch.nn.parallel import DistributedDataParallel
        from torch import nn, optim

        # Torch Example
        def train_func_distributed():
            num_epochs = 3
            model = NeuralNetwork()
            model = DistributedDataParallel(model)
            loss_fn = nn.MSELoss()
            optimizer = optim.SGD(model.parameters(), lr=0.1)

            for epoch in range(num_epochs):
                output = model(input)
                loss = loss_fn(output, labels)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                print(f"epoch: {epoch}, loss: {loss.item()}")

        from ray.train import Trainer

        trainer = Trainer(backend="torch", num_workers=4)
        trainer.start()
        results = trainer.run(train_func_distributed)
        trainer.shutdown()

If you are using PyTorch, you can use the :ref:`train-api-torch-utils` to automatically prepare your model & data loaders for distributed training.
If you are using Tensorflow, you would have to add ``MultiWorkerMirroredStrategy`` to your model in the training function instead of this automatically being done for you.

3. Rather than iteratively calling ``trainer.train()`` or ``trainer.validate()`` for each epoch, in Ray Train the training function defines the full training execution and is run via ``trainer.run(train_func)``.

In the following sections, we will guide you through the steps to migrate:

1. :ref:`sgd-migration-logic`
2. :ref:`Interacting with Trainer state (intermediate metrics, checkpointing) <sgd-migration-trainer>`
3. :ref:`Hyperparameter Tuning with Ray Tune <sgd-migration-tune>`

.. _sgd-migration-logic:

Training Logic
--------------
The main change you will have to make is how you define your training logic. In Ray SGD v1, the API for defining training logic differed for `TorchTrainer` vs. `TFTrainer`, so the steps to migrate will be different for each of these.

PyTorch
~~~~~~~
In v1, the training logic is defined through the ``train_epoch`` and ``train_batch`` methods of a ``TrainingOperator`` class which is passed into the ``TorchTrainer``. To migrate to Ray Train, there are 2 options:

1. If you felt the ``TrainingOperator`` is too unnecessary and complex, or you had to customize it extensively, you can define your own training function.
2. If you liked having your training logic in the ``TrainingOperator``, you can continue to use the ``TrainingOperator`` with Ray Train.

**Alternative 1: Custom Training Function**
You can define your own custom training function, and use only the parts from ``TrainingOperator.train_epoch``, ``TrainingOperator.setup``, and ``TrainingOperator.validate`` that are necessary for your application.

You can see a full example on how to :ref:`port over regular PyTorch DDP code to Ray Train here <train-porting-code>`

**Alternative 2: Continue to use TrainingOperator**
Alternatively, if you liked having the ``TrainingOperator``, you can define a training function that instantiates your `TrainingOperator` and you can call methods directly on the operator object.

So instead of

.. code-block:: python

    from ray.util.sgd import TrainingOperator, TorchTrainer

    class MyTrainingOperator(TrainingOperator):
       ...

    trainer = TorchTrainer(training_operator_cls=MyTrainingOperator, num_workers=4, use_gpu=True)

    num_epochs=10
    for _ in range(num_epochs):
        trainer.train()
        trainer.validate()

    final_model = trainer.get_model()


you would do

.. code-block:: python

    from ray.util.sgd import TrainingOperator
    from ray import train
    from ray.train import Trainer

    class MyTrainingOperator(TrainingOperator):
       ...

    def train_func(config):
       device = torch.device(f"cuda:{train.local_rank()}" if
                     torch.cuda.is_available() else "cpu")
       if torch.cuda.is_available():
           torch.cuda.set_device(device)

       # Set the args to whatever values you want.
       training_operator = MyTrainingOperator(
           config=config,
           world_rank=train.world_rank(),
           local_rank=train.local_rank(),
           is_distributed=True,
           device=device,
           use_gpu=True,
           wrap_ddp=True,
           add_dist_sampler=True

       training_operator.setup(config)

       for idx in range(config["num_epochs"]):
           train_loader = training_operator._get_train_loader()
           # If using DistributedSampler, set the epoch here.
           train_loader.set_epoch(idx)
           training_operator.train_epoch(epoch_idx=idx, iter(train_loader))

           validation_loader = training_operator._get_validation_loader()
           training_operator.validate(iterator=iter(validation_loader))

       if train.world_rank() == 0:
           return training_operator._get_original_models()
       else:
           return None

    trainer = Trainer(backend="torch", num_workers=4, use_gpu=True)
    trainer.start()
    results = trainer.run(train_func, config={"num_epochs": 10})
    final_model = results[0]

Tensorflow
~~~~~~~~~~

The API for ``TFTrainer`` uses creator functions instead of a ``TrainingOperator`` to define the training logic. To port over Ray SGD v1 Tensorflow code to Ray Train you can do the following:

.. code-block:: python

    from tensorflow.distribute import MultiWorkerMirroredStrategy

    from ray import train
    from ray.train import Trainer

    def train_func(config):
       train_dataset, val_dataset = data_creator(config)
       strategy = MultiWorkerMirroredStrategy()
       with strategy.scope():
           model = model_creator(config)

       for epoch_idx in range(config["num_epochs"]):
           model.fit(train_dataset)

       if train.world_rank() == 0:
           return model
       else:
           return None

    trainer = Trainer(backend="tensorflow", num_workers=4, config={"num_epochs": 3, ...})
    trainer.start()
    model = trainer.run(train_func)[0]

You can see a full example :ref:`here <train-porting-code>`.

.. _sgd-migration-trainer:

Interacting with the ``Trainer``
--------------------------------

In Ray SGD v1, you can iteratively call ``trainer.train()`` or ``trainer.validate()`` for each epoch, and can then interact with the trainer to get certain state (model, checkpoints, results, etc.). In Ray Train, this is replaced by a single training function that defines the full training & validation loop for all epochs.

There are 3 ways to get state during or after the training execution:


#. Return values from your training function
#. Intermediate results via ``train.report()``
#. Saving & loading checkpoints via ``train.save_checkpoint()`` and ``train.load_checkpoint()``

Return Values
~~~~~~~~~~~~~

To get any state from training *after* training has completed, you can simply return it from your training function. The return values from each the workers will be added to a list and returned from the ``trainer.run()`` call.

For example, to get the final model:

**Ray SGD v1**

.. code-block:: python

    from ray.util.sgd import TorchTrainer, TrainingOperator

    class MyTrainingOperator(TrainingOperator):
       ...

    trainer = TorchTrainer(training_operator_cls=MyTrainingOperator, num_workers=2)

    trainer.train()

    trained_model = trainer.get_model()

**Ray Train**

.. code-block:: python

    from ray.train import Trainer

    def train_func():
       model = Net()
       trainer_loader = MyDataset()
       for batch in train_loader:
           model.train(batch)

       return model

    trainer = Trainer(backend="torch")
    trainer.start()
    results = trainer.run(train_func, num_workers=2)
    assert len(results) == 2
    trained_model = results[0]

Intermediate Reporting
~~~~~~~~~~~~~~~~~~~~~~

If you want to access any values *during* the training process, you can do so via ``train.report()``. You can pass in any values to ``train.report()`` and these values from all workers will be sent to any callbacks passed into your ``Trainer``.

**Ray SGD v1**

.. code-block:: python

    from ray.util.sgd import TorchTrainer, TrainingOperator

    class MyTrainingOperator(TrainingOperator):
       ...

    trainer = TorchTrainer(training_operator_cls=MyTrainingOperator, num_workers=2)

    for _ in range(3):
        print(trainer.train(reduce_results=False))


**Ray Train**

.. code-block:: python

   from ray import train
   from ray.train import Trainer, TrainingCallback
   from typing import List, Dict

   class PrintingCallback(TrainingCallback):
       def handle_result(self, results: List[Dict], **info):
           print(results)

   def train_func():
       for i in range(3):
           train.report(epoch=i)

   trainer = Trainer(backend="torch", num_workers=2)
   trainer.start()
   result = trainer.run(
       train_func,
       callbacks=[PrintingCallback()]
   )
   # [{'epoch': 0, '_timestamp': 1630471763, '_time_this_iter_s': 0.0020279884338378906, '_training_iteration': 1}, {'epoch': 0, '_timestamp': 1630471763, '_time_this_iter_s': 0.0014922618865966797, '_training_iteration': 1}]
   # [{'epoch': 1, '_timestamp': 1630471763, '_time_this_iter_s': 0.0008401870727539062, '_training_iteration': 2}, {'epoch': 1, '_timestamp': 1630471763, '_time_this_iter_s': 0.0007486343383789062, '_training_iteration': 2}]
   # [{'epoch': 2, '_timestamp': 1630471763, '_time_this_iter_s': 0.0014500617980957031, '_training_iteration': 3}, {'epoch': 2, '_timestamp': 1630471763, '_time_this_iter_s': 0.0015292167663574219, '_training_iteration': 3}]
   trainer.shutdown()

See the :ref:`Ray Train User Guide <train-user-guide>` for more details.

Checkpointing
~~~~~~~~~~~~~

Finally, you can also use ``train.save_checkpoint()`` and ``train.load_checkpoint()`` to write checkpoints to disk during the training process, and to load from the most recently saved checkpoint in the case of node failures.

See the :ref:`Checkpointing <train-checkpointing>` and :ref:`Fault Tolerance & Elastic Training <train-fault-tolerance>` sections on the user guide for more info.

For example, in order to save checkpoints after every epoch:

**Ray SGD v1**

.. code-block:: python

    from ray.util.sgd import TorchTrainer, TrainingOperator

    class MyTrainingOperator(TrainingOperator):
       ...

    trainer = TorchTrainer(training_operator_cls=MyTrainingOperator, num_workers=2)

    for _ in range(3):
        trainer.train()
        trainer.save_checkpoint(checkpoint_dir="~/ray_results")


**Ray Train**

.. code-block:: python

    from ray import train
    from ray.train import Trainer

    def train_func():
       model = Net()
       trainer_loader = MyDataset()
       for i in range(3):
           for batch in train_loader:
               model.train(batch)
           train.save_checkpoint(epoch=i, model=model.state_dict()))

    trainer = Trainer(backend="torch")
    trainer.start()
    trainer.run(train_func, num_workers=2)


.. _sgd-migration-tune:

Hyperparameter Tuning with Ray Tune
-----------------------------------

Ray Train also comes with an easier to use interface for Hyperparameter Tuning with Ray Tune using Tune's function API instead of its Class API. In particular, it is much easier to define custom procedures because the logic is entirely defined by your training function.

There is a 1:1 mapping between rank 0 worker's ``train.report()``\ , ``train.save_checkpoint()``\ , and ``train.load_checkpoint()`` with ``tune.report()``\ , ``tune.save_checkpoint()``\ , and ``tune.load_checkpoint()``.

**Ray SGD v1**

.. code-block:: python

    from ray import tune
    from ray.util.sgd import TrainingOperator, TorchTrainer

    class MyTrainingOperator(TrainingOperator):
        ...

    def custom_step(trainer, info):
        train_stats = trainer.train()
        return train_stats

    # TorchTrainable is subclass of BaseTorchTrainable.
    TorchTrainable = TorchTrainer.as_trainable(
        training_operator_cls=MyTrainingOperator,
        num_workers=2,
        use_gpu=True,
        override_tune_step=custom_step
    )

    analysis = tune.run(
        TorchTrainable,
        config={"input": tune.grid_search([1, 2, 3])}
    )



**Ray Train**

.. code-block:: python

   from ray import train, tune
   from ray.train import Trainer

   def train_func(config)
       # In this example, nothing is expected to change over epochs,
       # and the output metric is equivalent to the input value.
       for _ in range(config["num_epochs"]):
           train.report(output=config["input"])

   trainer = Trainer(backend="torch", num_workers=2)
   trainable = trainer.to_tune_trainable(train_func)
   analysis = tune.run(trainable, config={
       "num_epochs": 2,
       "input": tune.grid_search([1, 2, 3])
   })
   print(analysis.get_best_config(metric="output", mode="max"))
   # {'num_epochs': 2, 'input': 3}

For more information see :ref:`train-tune`