import logging

import ray
import ray.util.collective as col

logger = logging.getLogger(__name__)

class BaseTrainer:
    def __init__(self, 
                 *, 
                 training_operator_cls, 
                 world_size=1, 
                 backend="auto", 
                 num_cpus_per_worker=1,
                 num_gpus_per_worker=1,
                 distributed_strategy="allreduce"
                 ):
    
        self.world_size = world_size
        self.backend = backend
        self.distributed_strategy = distributed_strategy

        if not ray.is_initialized() and self.max_replicas > 1:
            logger.info("Automatically initializing single-node Ray. To use "
                        "multi-node training, be sure to run `ray.init("
                        "address='auto')` before instantiating the Trainer.")
            ray.init()
        self._start_workers(training_operator_cls, 
                            num_cpus_per_worker, 
                            num_gpus_per_worker, 
                            world_size, 
                            backend)

        self._init_strategy(distributed_strategy)

    def _start_workers(self,
                       training_operator_cls, 
                       num_cpus_per_worker, 
                       num_gpus_per_worker, 
                       world_size, 
                       backend):
        """Create worker(actor), maybe need worker group to manager these workers.
           Or, send these workers to strategy to manager?

           set workers or worker group
           set worker info, record rank, backend, use_num_gpus?
        """
        pass

    def train(self):
        """Call operator train_one_epoch. Or run all epoches?
        """
        pass

    def validate(self):
        """Call operator validate to evaluate val_dataloader.
        """
        pass

    def step(self):
        """Call step in self.train(). different strategy calling here?
        """
        pass

    def state_dict(self):
        """return model parameter.
        """

    def load_state_dict(self, state_dict, blocking=False):
        """load model parameter.
        """

    def save(self, save_fn, checkpoint):
        """Saves the Trainer state to the provided checkpoint path.

        Args:
            save_fn (function): The saving function for different machine learning systems.
            checkpoint (str): Path to target checkpoint file.
        """
        save_fn(self.state_dict(), checkpoint)
        return checkpoint

    def shutdown(self, force=False):
        """Kill all workers.
        """
        pass