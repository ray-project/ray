import ray
from ray import tune


class LightningTrainable(tune.Trainable):
    '''
    This is an abstract class for trainable PyTorch Lightning models.
    This creator will internally use an accelerated Pytorch Lightning
    Trainer (`bigdl.nano.pytorch.Trainer`) when running on CPU, and the
    standard Pytorch Lightning Trainer when running on GPU.

    Generally you only need to implement `create_model` when subclassing
    LightningTrainable.

    Other implementation methods that may be helpful to override are
    `configure_trainer`

    A typical usage is:
    >>> class MyTrainable(LightningTrainable):
    >>>     def create_model(self, config):
    >>>         return PL_MODEL(config)
    >>> 
    >>> analysis = tune.run(MyTrainable, ...)

    :param cpu_binding: Users can set this class variable to enable or
           disable cpu core binding for each trial. Your Trainable without
           binding will totally rely on the OS scheduler, which is
           inefficient in most cases. With this variable set to True,
           trails will be binded to a fixed set of cpu id. It's highly
           recommeneded that users set `reuse_actor` to True as well if cpu
           binding is enabled. This technique is defaultly disabled.

    A typical usage for cpu_binding is:
    >>> class MyTrainable(LightningTrainable):
    >>>     cpu_binding = True
    >>>     ...

    '''

    _cpu_procs = None
    cpu_binding = False

    def create_model(self, config):
        '''
        Subclasses should override this method to return a PyTorch Lightning model

        `create_model` takes a config dictionary and returns a pytorch lightning module.
        '''
        raise NotImplementedError("Users need to implement this method")

    def configure_trainer(self):
        '''
        Subclasses should override this method to returns a dictionary to configure Trainer;
        please refer to
        https://pytorch-lightning.readthedocs.io/en/latest/common/trainer.html#trainer-class-api
        Users can also set parameters to configure some bigdl-nano pytorch-lightning parameters;
        please refer to
        https://bigdl.readthedocs.io/en/latest/doc/Nano/QuickStart/pytorch_train.html

        A default trainer setting will be {"max_epochs": 1}
        '''
        return {"max_epochs": 1}

    def setup(self, config):
        '''
        Subclasses should not override this method

        It creates an accelerated Pytorch Lightning Trainer (bigdl.nano.pytorch.Trainer)
        when running on CPU, and a standard Pytorch Lightning Trainer when running on GPU.
        '''
        self.model = self.create_model(config=config)
        trainer_config = self.configure_trainer()
        # TODO: change to an error raise method in ray if needed
        assert isinstance(trainer_config, dict),\
            f"`configure_trainer` should return a dict while get a {type(trainer_config)}"

        trial_required_resources = self.trial_resources.required_resources
        if "GPU" not in trial_required_resources:
            from bigdl.nano.pytorch import Trainer
            nano_config = {"use_ipex": True}
            nano_config.update(trainer_config)
            self.trainer = Trainer(**nano_config)
        else:
            from pytorch_lightning import Trainer
            self.trainer = Trainer(**trainer_config)

    def step(self):
        '''
        Subclasses should not override this method
        '''
        self.trainer.fit(self.model)
        valid_result = self.trainer.validate(self.model)
        return valid_result[0]

    def reset_config(self, config):
        '''
        Subclasses should not override this method
        '''
        self.setup(config)
        return True

    @classmethod
    def get_runtime_env(cls, placement_group_factory):
        '''
        Subclasses should not override this method.

        Set CPU specific tunings/optimizations in runtime_env
        when running on CPU.
        '''
        from bigdl.nano.common import get_nano_env_var
        # set CPU specific optimizations when running on CPU
        required_resources = placement_group_factory.required_resources
        if "GPU" in required_resources:
            return {}

        cpu_num = placement_group_factory.head_cpus
        nano_env = get_nano_env_var()
        # reset OMP_NUM_THREADS to be CPU required for each trial.
        nano_env["OMP_NUM_THREADS"] = str(int(cpu_num))
        # remove KMP_AFFINITY since it might not fit all cases
        nano_env.pop("KMP_AFFINITY", None)
        runtime_env = {"env_vars": nano_env}

        # cpu binding
        if cls.cpu_binding:
            from bigdl.nano.common.cpu_schedule import schedule_workers
            actor_num = int(ray.cluster_resources()["CPU"] // cpu_num)
            if cls._cpu_procs is None:
                cls._cpu_procs = iter(schedule_workers(actor_num))
            cpu_proc = next(cls._cpu_procs, None)
            if cpu_proc is not None:
                runtime_env["env_vars"]["KMP_AFFINITY"] =\
                                f"granularity=fine,proclist"\
                                f"=[{','.join([str(i) for i in cpu_proc])}],explicit"

        return runtime_env
