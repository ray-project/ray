.. _train-porting-code:

Converting an Existing Training Loop
====================================

The following instructions assume you have a training function
that can already be run on a single worker.


Updating your training function
-------------------------------

First, you'll want to update your training function to support distributed
training.


.. tab-set::

    .. tab-item:: PyTorch

        Ray Train will set up your distributed process group for you and also provides utility methods
        to automatically prepare your model and data for distributed training.

        .. note::
           Ray Train will still work even if you don't use the :func:`ray.train.torch.prepare_model`
           and :func:`ray.train.torch.prepare_data_loader` utilities below,
           and instead handle the logic directly inside your training function.

        First, use the :func:`~ray.train.torch.prepare_model` function to automatically move your model to the right device and wrap it in
        ``DistributedDataParallel``:

        .. code-block:: diff

             import torch
             from torch.nn.parallel import DistributedDataParallel
            +from ray.air import session
            +from ray import train
            +import ray.train.torch


             def train_func():
            -    device = torch.device(f"cuda:{train.get_context().get_local_rank()}" if
            -        torch.cuda.is_available() else "cpu")
            -    torch.cuda.set_device(device)

                 # Create model.
                 model = NeuralNetwork()

            -    model = model.to(device)
            -    model = DistributedDataParallel(model,
            -        device_ids=[train.get_context().get_local_rank()] if torch.cuda.is_available() else None)

            +    model = train.torch.prepare_model(model)

                 ...



        Then, use the ``prepare_data_loader`` function to automatically add a ``DistributedSampler`` to your ``DataLoader``
        and move the batches to the right device. This step is not necessary if you are passing in Ray Data to your Trainer
        (see :ref:`data-ingest-torch`):

        .. code-block:: diff

             import torch
             from torch.utils.data import DataLoader, DistributedSampler
            + from ray import train
            + import ray.train.torch


             def train_func():
            -    device = torch.device(f"cuda:{train.get_context().get_local_rank()}" if
            -        torch.cuda.is_available() else "cpu")
            -    torch.cuda.set_device(device)

                 ...

            -    data_loader = DataLoader(my_dataset, batch_size=worker_batch_size, sampler=DistributedSampler(dataset))

            +    data_loader = DataLoader(my_dataset, batch_size=worker_batch_size)
            +    data_loader = train.torch.prepare_data_loader(data_loader)

                 for X, y in data_loader:
            -        X = X.to_device(device)
            -        y = y.to_device(device)

        .. tip::
            Keep in mind that ``DataLoader`` takes in a ``batch_size`` which is the batch size for each worker.
            The global batch size can be calculated from the worker batch size (and vice-versa) with the following equation:

            .. code-block:: python

                global_batch_size = worker_batch_size * train.get_context().get_world_size()
    
    .. tab-item:: PyTorch Lightning

        Ray Train will set up your distributed process group on each worker. You only need to 
        make a few changes to your Lightning Trainer definition.

        .. code-block:: diff

              import pytorch_lightning as pl
            + from ray.train.lightning import (
            +   prepare_trainer,
            +   RayDDPStrategy,
            +   RayLightningEnvironment,
            + )

              def train_func_per_worker():
                ...
                model = MyLightningModule(...)
                datamodule = MyLightningDataModule(...)
                
                trainer = pl.Trainer(
            -     devices=[0,1,2,3],
            -     strategy=DDPStrategy(),
            -     plugins=[LightningEnvironment()],
            +     devices="auto",
            +     strategy=RayDDPStrategy(),
            +     plugins=[RayLightningEnvironment()]
                )
            +   trainer = prepare_trainer(trainer)
                
                trainer.fit(model, datamodule=datamodule)
    

        **Step 1: Configure Distributed Strategy**

        Ray Train offers several subclassed distributed strategies for Lightning. 
        These strategies retain the same argument list as their base strategy classes. 
        Internally, they configure the root device and the distributed 
        sampler arguments.
            
        - :class:`~ray.train.lightning.RayDDPStrategy` 
        - :class:`~ray.train.lightning.RayFSDPStrategy` 
        - :class:`~ray.train.lightning.RayDeepSpeedStrategy` 

        **Step 2: Configure Ray Cluster Environment Plugin**

        Ray Train also provides :class:`~ray.train.lightning.RayLightningEnvironment` 
        as a specification for Ray Cluster. This utility class configures the worker's 
        local, global, and node rank and world size.

        **Step 3: Configure Parallel Devices**
        
        In addition, Ray TorchTrainer has already configured the correct 
        ``CUDA_VISIBLE_DEVICES`` for you. One should always use all available 
        GPUs by setting ``devices="auto"``.
        
        **Step 4: Prepare your Lightning Trainer**

        Finally, pass your Lightning Trainer into
        :meth:`~ray.train.lightning.prepare_trainer` to validate 
        your configurations. 



Creating a :class:`~ray.train.torch.TorchTrainer`
-------------------------------------------------

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. For distributed PyTorch, we use a :class:`~ray.train.torch.TorchTrainer`
that you can setup like this:


.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )



To customize the backend setup, you can pass a
:class:`~ray.train.torch.TorchConfig`:

.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer, TorchConfig

    trainer = TorchTrainer(
        train_func,
        torch_backend=TorchConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )

For more configurability, please reference the :py:class:`~ray.train.data_parallel_trainer.DataParallelTrainer` API.

Running your training function
------------------------------

With a distributed training function and a Ray Train ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()


Configuring Training
--------------------

With Ray Train, you can execute a training function (``train_func``) in a
distributed manner by calling ``Trainer.fit``. To pass arguments
into the training function, you can expose a single ``config`` dictionary parameter:

.. code-block:: diff

    -def train_func():
    +def train_func(config):

Then, you can pass in the config dictionary as an argument to ``Trainer``:

.. code-block:: diff

    +config = {} # This should be populated.
     trainer = TorchTrainer(
         train_func,
    +    train_loop_config=config,
         scaling_config=ScalingConfig(num_workers=2)
     )

Putting this all together, you can run your training function with different
configurations. As an example:

.. code-block:: python

    from ray import train
    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        for i in range(config["num_epochs"]):
            train.report({"epoch": i})

    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 2},
        scaling_config=ScalingConfig(num_workers=2)
    )
    result = trainer.fit()
    print(result.metrics["num_epochs"])
    # 1

A primary use-case for ``config`` is to try different hyperparameters. To
perform hyperparameter tuning with Ray Train, please refer to the
:ref:`Ray Tune integration <train-tune>`.


.. _train-result-object:

Accessing Training Results
--------------------------

.. TODO(ml-team) Flesh this section out.

The return of a ``Trainer.fit`` is a :py:class:`~ray.air.result.Result` object, containing
information about the training run. You can access it to obtain saved checkpoints,
metrics and other relevant data.

For example, you can:

* Print the metrics for the last training iteration:

.. code-block:: python

    from pprint import pprint

    pprint(result.metrics)
    # {'_time_this_iter_s': 0.001016855239868164,
    #  '_timestamp': 1657829125,
    #  '_training_iteration': 2,
    #  'config': {},
    #  'date': '2022-07-14_20-05-25',
    #  'done': True,
    #  'episodes_total': None,
    #  'epoch': 1,
    #  'experiment_id': '5a3f8b9bf875437881a8ddc7e4dd3340',
    #  'experiment_tag': '0',
    #  'hostname': 'ip-172-31-43-110',
    #  'iterations_since_restore': 2,
    #  'node_ip': '172.31.43.110',
    #  'pid': 654068,
    #  'time_since_restore': 3.4353830814361572,
    #  'time_this_iter_s': 0.00809168815612793,
    #  'time_total_s': 3.4353830814361572,
    #  'timestamp': 1657829125,
    #  'timesteps_since_restore': 0,
    #  'timesteps_total': None,
    #  'training_iteration': 2,
    #  'trial_id': '4913f_00000',
    #  'warmup_time': 0.003167867660522461}

* View the dataframe containing the metrics from all iterations:

.. code-block:: python

    print(result.metrics_dataframe)

* Obtain the :py:class:`~ray.air.checkpoint.Checkpoint`, used for resuming training, prediction and serving.

.. code-block:: python

    result.checkpoint  # last saved checkpoint
    result.best_checkpoints  # N best saved checkpoints, as configured in run_config
    result.error  # returns the Exception if training failed.


See :class:`the Result docstring <ray.air.result.Result>` for more details.

.. _train-huggingface:

Hugging Face
------------

TransformersTrainer
~~~~~~~~~~~~~~~~~~~

:class:`TransformersTrainer <ray.train.huggingface.TransformersTrainer>` further extends :class:`TorchTrainer <ray.train.torch.TorchTrainer>`, built
for interoperability with the HuggingFace Transformers library.

Users are required to provide a ``trainer_init_per_worker`` function which returns a
``transformers.Trainer`` object. The ``trainer_init_per_worker`` function
will have access to preprocessed train and evaluation datasets.

Upon calling `TransformersTrainer.fit()`, multiple workers (ray actors) will be spawned,
and each worker will create its own copy of a ``transformers.Trainer``.

Each worker will then invoke ``transformers.Trainer.train()``, which will perform distributed
training via Pytorch DDP.


.. dropdown:: Code example

    .. literalinclude:: ../doc_code/hf_trainer.py
        :language: python
        :start-after: __hf_trainer_start__
        :end-before: __hf_trainer_end__

AccelerateTrainer
~~~~~~~~~~~~~~~~~

If you prefer a more fine-grained Hugging Face API than what Transformers provides, you can use :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>`
to run training functions making use of Hugging Face Accelerate. Similarly to :class:`TransformersTrainer <ray.train.huggingface.TransformersTrainer>`, :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>`
is also an extension of :class:`TorchTrainer <ray.train.torch.TorchTrainer>`.

:class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>` allows you to pass an Accelerate configuration file generated with ``accelerate config`` to be applied on all training workers.
This ensures that the worker environments are set up correctly for Accelerate, allowing you to take advantage of Accelerate APIs and integrations such as DeepSpeed and FSDP
just as you would if you were running Accelerate without Ray.

.. note::
    ``AccelerateTrainer`` will override some settings set with ``accelerate config``, mainly related to
    the topology and networking. See the :class:`AccelerateTrainer <ray.train.huggingface.AccelerateTrainer>`
    API reference for more details.

Aside from Accelerate support, the usage is identical to :class:`TorchTrainer <ray.train.torch.TorchTrainer>`, meaning you define your own training function
and use the :func:`~ray.train.report` API to report metrics, save checkpoints etc.


.. dropdown:: Code example

    .. literalinclude:: ../doc_code/accelerate_trainer.py
        :language: python
