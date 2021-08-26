import ray
from pytorch_lightning.accelerators.horovod_accelerator import \
    HorovodAccelerator

try:
    import horovod.torch as hvd
    from horovod.ray import RayExecutor
except (ModuleNotFoundError, ImportError):
    HOROVOD_AVAILABLE = False
else:
    HOROVOD_AVAILABLE = True


def get_executable_cls():
    # Only used for testing purposes, currently.
    # We need to override this in tests to ensure test path is set correctly.
    return None


class HorovodRayAccelerator(HorovodAccelerator):
    """Pytorch Lightning Accelerator for Horovod training on a Ray cluster.

    This accelerator is used to manage distributed training on a Ray cluster
    via the Horovod training framework. Internally, the specified number of
    Ray actors are launched in the cluster and are configured as part of the
    Horovod ring. The Pytorch Lightning trainer is instantiated on the
    driver and sent to each of these training workers where training is
    executed. The distributed training protocol is handled by Horovod.

    Each training worker is configured to reserve 1 CPU and if 1 GPU if
    ``use_gpu`` is set to ``True``.

    If using this accelerator, you should run your code like a normal Python
    script: ``python train.py``, and not with ``horovodrun``.

    Args:
        num_hosts (int): The number of nodes/machines to execute the job on.
        num_slots (int): Number of workers to be placed on each machine.
        use_gpu (bool): Whether to use GPU for allocation. For GPU to be
            used, you must also set the ``gpus`` arg in your Pytorch Lightning
            Trainer to a value > 0.

    Example:

        .. code_block:: python

            import pytorch_lightning as ptl
            from ray.util.lightning_accelerators import HorovodRayAccelerator

            ptl_model = MNISTClassifier(...)
            # 2 nodes, 4 workers per node, each using 1 CPU and 1 GPU.
            accelerator = HorovodRayAccelerator(num_hosts=2, num_slots=4,
                use_gpu=True).

            # If using GPUs, set the ``gpus`` arg to a value > 0.
            # The actual number of GPUs is determined by ``num_slots``.
            trainer = pl.Trainer(..., gpus=1, accelerator=accelerator).
            trainer.fit(ptl_model).

    """

    def __init__(self,
                 *args,
                 num_hosts=1,
                 num_slots=1,
                 use_gpu=False,
                 **kwargs):
        super().__init__(*args, trainer=None, **kwargs)
        self.nickname = "horovod_ray"
        self.num_hosts = num_hosts
        self.num_slots = num_slots
        self.use_gpu = use_gpu

    def setup(self, model):
        self.trainer.use_horovod = True
        settings = RayExecutor.create_settings(timeout_s=30)
        self.executor = RayExecutor(
            settings,
            num_hosts=self.num_hosts,
            num_slots=self.num_slots,
            use_gpu=self.use_gpu)
        self.trainer.model = model
        self.executor.start(executable_cls=get_executable_cls())

    def train(self):
        trainer = self.trainer
        trainer_ref = ray.put(self.trainer)
        self.trainer = None
        results = self.executor.run(self.train_remote, args=[trainer_ref])
        results, state_dict, best_path = results[0]

        self.trainer = trainer
        self.trainer.model.load_state_dict(state_dict)
        if self.trainer.checkpoint_callback:
            self.trainer.checkpoint_callback.best_model_path = best_path

        return results

    def train_remote(self, trainer_ref):
        self.trainer = ray.get(trainer_ref)
        hvd.init()
        if self.trainer.on_gpu:
            # Horovod assigns one local GPU per process.
            self.trainer.root_gpu = hvd.local_rank()

        # TODO: Make changes in PTL to clean this up.
        super(HorovodRayAccelerator, self).setup(self.trainer.model)
        results = super(HorovodRayAccelerator, self).train()
        if hvd.rank() != 0:
            # Only want results from the first worker.
            return None

        best_model_path = None
        if self.trainer.checkpoint_callback is not None:
            best_model_path = self.trainer.checkpoint_callback.best_model_path

        model = self.trainer.model
        return results, model.state_dict(), best_model_path

    def teardown(self):
        self.executor.shutdown()
