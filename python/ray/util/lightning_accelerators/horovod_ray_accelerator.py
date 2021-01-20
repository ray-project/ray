from pytorch_lightning.accelerators.horovod_accelerator import HorovodAccelerator

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
    def __init__(self, trainer=None, cluster_environment=None):
        super().__init__(trainer, cluster_environment)
        self.nickname = "horovod_ray"

    def setup(self, model):
        self.trainer.use_horovod = True
        settings = RayExecutor.create_settings(timeout_s=30)
        num_hosts = self.trainer.num_nodes
        use_gpu = self.trainer.on_gpu
        if use_gpu:
            num_slots = self.trainer.num_gpus
        else:
            num_slots = self.trainer.num_processes
        self.executor = RayExecutor(
            settings,
            num_hosts=num_hosts,
            num_slots=num_slots,
            use_gpu=use_gpu)
        self.trainer.model = model
        self.executor.start(executable_cls=get_executable_cls())

    def train(self):
        results = self.executor.run(self.train_remote)
        results, state_dict, best_path = results[0]

        self.trainer.model.load_state_dict(state_dict)
        if self.trainer.checkpoint_callback:
            self.trainer.checkpoint_callback.best_model_path = best_path

        return results

    def train_remote(self):
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
