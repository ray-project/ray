.. _train-checkpointing:

Saving and Loading Checkpoints
==============================

Ray Train provides a way to snapshot training progress with :class:`Checkpoints <ray.train.Checkpoint>`.

This is useful for:

1. **Storing the best-performing model weights:** Save your model to persistent storage, and use it for downstream serving/inference.
2. **Fault tolerance:** Handle node failures in a long-running training job on a cluster of pre-emptible machines/pods.
3. **Distributed checkpointing:** When doing *model-parallel training*, Ray Train checkpointing provides an easy way to
   :ref:`upload model shards from each worker in parallel <train-distributed-checkpointing>`,
   without needing to gather the full model to a single node.
4. **Integration with Ray Tune:** Checkpoint saving and loading is required by certain :ref:`Ray Tune schedulers <tune-schedulers>`.


.. _train-dl-saving-checkpoints:

Saving checkpoints during training
----------------------------------

The :class:`Checkpoint <ray.train.Checkpoint>` is a lightweight interface provided
by Ray Train that represents a *directory* that exists at some ``(filesystem, path)``.

For example, a checkpoint in cloud storage would be represented by
``Checkpoint(filesystem=S3FileSystem, path="my-bucket/my-checkpoint")``.
A locally available checkpoint would be represented by
``Checkpoint(filesystem=LocalFileSystem, path="/tmp/my-checkpoint")``.

Here's how you modify your training loop to save a checkpoint in Ray Train:

1. Write your model checkpoint to a local directory.

   - Since a :class:`Checkpoint <ray.train.Checkpoint>` just points to a directory, the contents are completely up to you.
   - This means that you can use any serialization format you want.
   - This makes it **easy to use familiar checkpoint utilities provided by training frameworks**, such as
     ``torch.save``, ``pl.Trainer.save_checkpoint``, Accelerate's ``accelerator.save_model``,
     Transformers' ``save_pretrained``, and ``tf.keras.Model.save``.

2. Create a :class:`Checkpoint <ray.train.Checkpoint>` from the directory using :meth:`Checkpoint.from_directory <ray.train.Checkpoint.from_directory>`.

3. Report the checkpoint to Ray Train using :func:`ray.train.report(metrics, checkpoint=...) <ray.train.report>`.

   - The metrics reported alongside the checkpoint are used to :ref:`keep track of the best-performing checkpoints <train-dl-configure-checkpoints>`.
   - This will **upload the checkpoint to persistent storage** if configured. See :ref:`persistent-storage-guide`.


.. figure:: ../images/checkpoint_lifecycle.png

    The lifecycle of a :class:`~ray.train.Checkpoint`, from being saved locally
    to disk to being uploaded to persistent storage via ``train.report``.

.. tip::

    In standard DDP training, where each worker has a copy of the full-model, you should
    only save and report a checkpoint from a single worker to prevent redundant uploads.

    This typically looks like:

    .. literalinclude::  ../doc_code/checkpoints.py
        :language: python
        :start-after: __checkpoint_from_single_worker_start__
        :end-before: __checkpoint_from_single_worker_end__

    If using parallel training strategies such as DeepSpeed Zero-3 and FSDP, where
    each worker only has a shard of the full-model, you should save and report a checkpoint
    from each worker. See :ref:`train-distributed-checkpointing` for an example.


Here are a few examples of saving checkpoints with different training frameworks:

.. tab-set::

    .. tab-item:: Native PyTorch

        .. literalinclude:: ../doc_code/checkpoints.py
            :language: python
            :start-after: __pytorch_save_start__
            :end-before: __pytorch_save_end__

        .. tip::

            You should unwrap the DDP model before saving it to a checkpoint.

            ``model.module`` is the original model before wrapping it with DDP.


    .. tab-item:: PyTorch Lightning

        Ray Train leverages PyTorch Lightning's ``Callback`` interface to report metrics
        and checkpoints. We provide a simple callback implementation that reports
        ``on_train_epoch_end``.

        Specifically, on each train epoch end, it

        - collects all the logged metrics from ``trainer.callback_metrics``
        - saves a checkpoint via ``trainer.save_checkpoint``
        - reports to Ray Train via :func:`ray.train.report(metrics, checkpoint) <ray.train.report>`

        .. literalinclude:: ../doc_code/checkpoints.py
            :language: python
            :start-after: __lightning_save_example_start__
            :end-before: __lightning_save_example_end__

        You can always get the saved checkpoint path from :attr:`result.checkpoint <ray.train.Result.checkpoint>` and
        :attr:`result.best_checkpoints <ray.train.Result.best_checkpoints>`.

        For more advanced usage (e.g. reporting at different frequency, reporting
        customized checkpoint files), you can implement your own customized callback.
        Here is a simple example that reports a checkpoint every 3 epochs:

        .. literalinclude:: ../doc_code/checkpoints.py
            :language: python
            :start-after: __lightning_custom_save_example_start__
            :end-before: __lightning_custom_save_example_end__


    .. tab-item:: Hugging Face Transformers

        Ray Train leverages HuggingFace Transformers Trainer's ``Callback`` interface
        to report metrics and checkpoints.

        **Option 1: Use Ray Train's default report callback**

        We provide a simple callback implementation :class:`~ray.train.huggingface.transformers.RayTrainReportCallback` that
        reports on checkpoint save. You can change the checkpointing frequency by ``save_strategy`` and ``save_steps``.
        It collects the latest logged metrics and report them together with the latest saved checkpoint.

        .. literalinclude:: ../doc_code/checkpoints.py
            :language: python
            :start-after: __transformers_save_example_start__
            :end-before: __transformers_save_example_end__

        Note that :class:`~ray.train.huggingface.transformers.RayTrainReportCallback`
        binds the latest metrics and checkpoints together,
        so users can properly configure ``logging_strategy``, ``save_strategy`` and ``evaluation_strategy``
        to ensure the monitoring metric is logged at the same step as checkpoint saving.

        For example, the evaluation metrics (``eval_loss`` in this case) are logged during
        evaluation. If users want to keep the best 3 checkpoints according to ``eval_loss``, they
        should align the saving and evaluation frequency. Below are two examples of valid configurations:

        .. code-block:: python

            args = TrainingArguments(
                ...,
                evaluation_strategy="epoch",
                save_strategy="epoch",
            )

            args = TrainingArguments(
                ...,
                evaluation_strategy="steps",
                save_strategy="steps",
                eval_steps=50,
                save_steps=100,
            )

            # And more ...


        **Option 2: Implement your customized report callback**

        If you feel that Ray Train's default :class:`~ray.train.huggingface.transformers.RayTrainReportCallback`
        is not sufficient for your use case, you can also implement a callback yourself!
        Below is a example implementation that collects latest metrics
        and reports on checkpoint save.

        .. literalinclude:: ../doc_code/checkpoints.py
            :language: python
            :start-after: __transformers_custom_save_example_start__
            :end-before: __transformers_custom_save_example_end__


        You can customize when (``on_save``, ``on_epoch_end``, ``on_evaluate``) and
        what (customized metrics and checkpoint files) to report by implementing your own
        Transformers Trainer callback.


.. _train-distributed-checkpointing:

Saving checkpoints from multiple workers (distributed checkpointing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In model parallel training strategies where each worker only has a shard of the full-model,
you can save and report checkpoint shards in parallel from each worker.

.. figure:: ../images/persistent_storage_checkpoints.png

    Distributed checkpointing in Ray Train. Each worker uploads its own checkpoint shard
    to persistent storage independently.

Distributed checkpointing is the best practice for saving checkpoints
when doing model-parallel training (e.g., DeepSpeed, FSDP, Megatron-LM).

There are two major benefits:

1. **It is faster, resulting in less idle time.** Faster checkpointing incentivizes more frequent checkpointing!

   Each worker can upload its checkpoint shard in parallel,
   maximizing the network bandwidth of the cluster. Instead of a single node
   uploading the full model of size ``M``, the cluster distributes the load across
   ``N`` nodes, each uploading a shard of size ``M / N``.

2. **Distributed checkpointing avoids needing to gather the full model onto a single worker's CPU memory.**

   This gather operation puts a large CPU memory requirement on the worker that performs checkpointing
   and is a common source of OOM errors.


Here is an example of distributed checkpointing with PyTorch:

.. literalinclude:: ../doc_code/checkpoints.py
    :language: python
    :start-after: __distributed_checkpointing_start__
    :end-before: __distributed_checkpointing_end__


.. _train-dl-configure-checkpoints:

Configure checkpointing
-----------------------

For more configurability of checkpointing behavior (specifically saving
checkpoints to disk), a :py:class:`~ray.train.CheckpointConfig` can be passed into
``Trainer``.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_start__
    :end-before: __checkpoint_config_end__


.. seealso::

    See the :class:`~ray.train.CheckpointConfig` API reference.

.. note::

    If you want to save the top-k checkpoints with respect to a metric via
    :py:class:`~ray.train.CheckpointConfig`,
    please ensure that the metric is always reported together with the checkpoints.



Using checkpoints after training
--------------------------------

The latest saved checkpoint can be accessed with :attr:`Result.checkpoint <ray.train.Result.checkpoint>`.

The full list of persisted checkpoints can be accessed with :attr:`Result.best_checkpoints <ray.train.Result.best_checkpoints>`.
If :class:`CheckpointConfig(num_to_keep) <ray.train.CheckpointConfig>` is set, this list will contain the best ``num_to_keep`` checkpoints.

See :ref:`train-inspect-results` for a full guide on inspecting training results.

:meth:`Checkpoint.as_directory <ray.train.Checkpoint.as_directory>`
and :meth:`Checkpoint.to_directory <ray.train.Checkpoint.to_directory>`
are the two main APIs to interact with Train checkpoints.


.. _train-dl-loading-checkpoints:

Restore training state from a checkpoint
----------------------------------------

:class:`Checkpoints <ray.train.Checkpoint>` can be accessed in the training function with :func:`ray.train.get_checkpoint <ray.train.get_checkpoint>`.

The checkpoint can be populated in two ways:

1. It can be auto-populated, e.g. for :ref:`automatic failure recovery <train-fault-tolerance>` or :ref:`on manual restoration <train-restore-guide>`.
2. The checkpoint can be passed to the :class:`Trainer <ray.train.trainer.BaseTrainer>` as the ``resume_from_checkpoint`` argument.


.. tab-set::

    .. tab-item:: Native PyTorch

        .. code-block:: python
            :emphasize-lines: 23, 25, 26, 29, 30, 31, 35

            import ray.train.torch
            from ray import train
            from ray.train import Checkpoint, ScalingConfig
            from ray.train.torch import TorchTrainer

            import torch
            import torch.nn as nn
            from torch.optim import Adam
            import numpy as np

            def train_func(config):
                n = 100
                # create a toy dataset
                # data   : X - dim = (n, 4)
                # target : Y - dim = (n, 1)
                X = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
                Y = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))

                # toy neural network : 1-layer
                model = nn.Linear(4, 1)
                criterion = nn.MSELoss()
                optimizer = Adam(model.parameters(), lr=3e-4)
                start_epoch = 0

                checkpoint = train.get_checkpoint()
                if checkpoint:
                    # assume that we have run the train.report() example
                    # and successfully save some model weights
                    checkpoint_dict = checkpoint.to_dict()
                    model.load_state_dict(checkpoint_dict.get("model_weights"))
                    start_epoch = checkpoint_dict.get("epoch", -1) + 1

                # wrap the model in DDP
                model = ray.train.torch.prepare_model(model)
                for epoch in range(start_epoch, config["num_epochs"]):
                    y = model.forward(X)
                    # compute loss
                    loss = criterion(y, Y)
                    # back-propagate loss
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()
                    state_dict = model.state_dict()
                    checkpoint = Checkpoint.from_dict(
                        dict(epoch=epoch, model_weights=state_dict)
                    )
                    train.report({}, checkpoint=checkpoint)

            trainer = TorchTrainer(
                train_func,
                train_loop_config={"num_epochs": 2},
                scaling_config=ScalingConfig(num_workers=2),
            )
            # save a checkpoint
            result = trainer.fit()

            # load checkpoint
            trainer = TorchTrainer(
                train_func,
                train_loop_config={"num_epochs": 4},
                scaling_config=ScalingConfig(num_workers=2),
                resume_from_checkpoint=result.checkpoint,
            )
            result = trainer.fit()

            print(result.checkpoint.to_dict())
            # {'epoch': 3, 'model_weights': OrderedDict([('bias', tensor([0.0902])), ('weight', tensor([[-0.1549, -0.0861,  0.4353, -0.4116]]))]), '_timestamp': 1656108265, '_preprocessor': None, '_current_checkpoint_id': 2}

    .. tab-item:: PyTorch Lightning

        .. code-block:: python
            :emphasize-lines: 11-17

            from ray import train
            from ray.train import Checkpoint, ScalingConfig
            from ray.train.torch import TorchTrainer
            from ray.train.lightning import RayTrainReportCallback
            from os.path import join

            def train_func_per_worker():
                model = MyLightningModule(...)
                datamodule = MyLightningDataModule(...)
                trainer = pl.Trainer(
                    ...
                    callbacks=[RayTrainReportCallback()]
                )

                checkpoint = train.get_checkpoint()
                if checkpoint:
                    with checkpoint.as_directory() as ckpt_dir:
                        ckpt_path = join(ckpt_dir, "checkpoint.ckpt")
                        trainer.fit(model, datamodule=datamodule, ckpt_path=ckpt_path)
                else:
                    trainer.fit(model, datamodule=datamodule)

            # Build a Ray Train Checkpoint
            # Suppose we have a Lightning checkpoint under ./ckpt_dir/checkpoint.ckpt
            checkpoint = Checkpoint.from_directory("./ckpt_dir/checkpoint.ckpt")

            # Resume training from checkpoint file
            ray_trainer = TorchTrainer(
                train_func_per_worker,
                scaling_config=ScalingConfig(num_workers=2),
                resume_from_checkpoint=checkpoint,
            )
            result = ray_trainer.fit()

