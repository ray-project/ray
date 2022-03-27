from typing import Dict, Callable, Optional, Union

from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.trainer import GenDataset
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint


from ray.ml.train.data_parallel_trainer import DataParallelTrainer
from ray.train.horovod import HorovodConfig


class HorovodTrainer(DataParallelTrainer):
    """A Trainer for data parallel Horovod training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary Horovod process group already
    configured for distributed Horovod training.

    The ``train_loop_per_worker`` function is expected to take in either 0 or 1
    arguments:

    .. code-block:: python

        def train_loop_per_worker():
            ...

    .. code-block:: python

        def train_loop_per_worker(config: Dict):
            ...

    If ``train_loop_per_worker`` accepts an argument, then
    ``train_loop_config`` will be passed in as the argument. This is useful if you
    want to tune the values in ``train_loop_config`` as hyperparameters.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards that can then be accessed by ``ray.train.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``ray.train.get_dataset_shard(...)`` will return the the entire Dataset.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray Train function utils <train-api-func-utils>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging.
            train.report(...)

            # Checkpoints the provided args as restorable state.
            train.save_checkpoint(...)

            # Returns dict of last saved checkpoint.
            train.load_checkpoint()

            # Returns the Ray Dataset shard for the given key.
            train.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            train.get_world_size()

            # Returns the rank of this worker.
            train.get_world_rank()

            # Returns the rank of the worker on the current node.
            train.get_local_rank()

    You could use ``TensorflowPredictor`` or ``TorchPredictor`` in conjunction with
    HorovodTrainer. You must save it under the "model" kwarg in
    ``train.save_checkpoint()``.

    Example:

    .. code-block:: python

        class Net(nn.Module):
            def __init__(self):
                super(Net, self).__init__()
                self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
                self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
                self.conv2_drop = nn.Dropout2d()
                self.fc1 = nn.Linear(320, 50)
                self.fc2 = nn.Linear(50, 10)

            def forward(self, x):
                x = F.relu(F.max_pool2d(self.conv1(x), 2))
                x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
                x = x.view(-1, 320)
                x = F.relu(self.fc1(x))
                x = F.dropout(x, training=self.training)
                x = self.fc2(x)
                return F.log_softmax(x)


        def setup(config):
            batch_size = config.get("batch_size", 64)
            use_adasum = config.get("use_adasum", False)
            lr = config.get("lr", 0.01)
            momentum = config.get("momentum", 0.5)
            use_cuda = config.get("use_cuda", False)

            # Horovod: initialize library.
            hvd.init()

            if use_cuda:
                # Horovod: pin GPU to local rank.
                torch.cuda.set_device(hvd.local_rank())

            # Horovod: limit # of CPU threads to be used per worker.
            torch.set_num_threads(1)

            kwargs = {"num_workers": 1, "pin_memory": True} if use_cuda else {}
            with FileLock(os.path.expanduser("~/.horovod_lock")):
                train_dataset = datasets.MNIST(
                    "~/data",
                    train=True,
                    download=True,
                    transform=transforms.Compose(
                        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]  # noqa
                    ),
                )
            # Horovod: use DistributedSampler to partition the training data.
            train_sampler = torch.utils.data.distributed.DistributedSampler(
                train_dataset, num_replicas=hvd.size(), rank=hvd.rank()
            )
            train_loader = torch.utils.data.DataLoader(
                train_dataset, batch_size=batch_size, sampler=train_sampler, **kwargs
            )

            model = Net()

            # By default, Adasum doesn't need scaling up learning rate.
            lr_scaler = hvd.size() if not use_adasum else 1

            if use_cuda:
                # Move model to GPU.
                model.cuda()
                # If using GPU Adasum allreduce, scale learning rate by local_size.
                if use_adasum and hvd.nccl_built():
                    lr_scaler = hvd.local_size()

            # Horovod: scale learning rate by lr_scaler.
            optimizer = optim.SGD(model.parameters(), lr=lr * lr_scaler,
                momentum=momentum)

            # Horovod: wrap optimizer with DistributedOptimizer.
            optimizer = hvd.DistributedOptimizer(
                optimizer,
                named_parameters=model.named_parameters(),
                op=hvd.Adasum if use_adasum else hvd.Average,
            )

            return model, optimizer, train_loader, train_sampler


        def train_epoch(
            model, optimizer, train_sampler, train_loader, epoch, use_cuda
        ):
            loss = None
            model.train()
            # Horovod: set epoch to sampler for shuffling.
            train_sampler.set_epoch(epoch)
            for batch_idx, (data, target) in enumerate(train_loader):
                if use_cuda:
                    data, target = data.cuda(), target.cuda()
                optimizer.zero_grad()
                output = model(data)
                loss = F.nll_loss(output, target)
                loss.backward()
                optimizer.step()
            return loss.item() if loss else None


        def train_func(config):
            use_cuda = config.get("use_cuda", False)

            model, optimizer, train_loader, train_sampler = setup(config)

            results = []
            for epoch in range(10):
                loss = train_epoch(
                    model, optimizer, train_sampler, train_loader, epoch, use_cuda
                )
                results.append(loss)
            return results

        scaling_config = {"num_workers": num_workers}
        config = {"use_cuda": False}
        trainer = HorovodTrainer(
            train_loop_per_worker=train_func,
            train_loop_config=config,
            scaling_config=scaling_config,
        )
        result = trainer.fit()


    Args:
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        horovod_config: Configuration for setting up the Horovod backend.
            If set to None, use the default configuration. This replaces the
            ``backend_config`` arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        train_loop_config: Optional[Dict] = None,
        horovod_config: Optional[HorovodConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        super().__init__(
            train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=horovod_config or HorovodConfig(),
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )
