import logging
import numpy as np
import os
import time
import torch
import torch.nn as nn
import argparse
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining
from torch.utils.data import DataLoader, Subset
from torchvision.datasets import CIFAR10
import torchvision.transforms as transforms

from functional import seq
import ray
from ray.tune import CLIReporter
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import BATCH_SIZE
from ray.tune.resources import Resources
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.trial import Trial
from ray.tune.utils.util import merge_dicts

logger = logging.getLogger(__name__)

def in_state(*states):
    return lambda trial: trial.status in states

class FailureInjector:
    def __init__(self, duration_s=120, keep_nodes=True):
        #self._failure_points = failure_points
        #self._random = random
        self._duration_s = duration_s
        self._keep_nodes = keep_nodes
        #logger.warning(f"Will inject failures at {failure_points}")

    def inject(self, ip_address, provisioner):
        # Kill node at ip_address for duration_s seconds.
        provisioner.scaler.inject_failure(ip_address, self._duration_s)

    def target(self, trials, timestamp=None):
        """Which node to kill?"""
        # if not self._failure_points:
        #     return None
        # if timestamp < self._failure_points[0]:
        #     return None

        #self._failure_points.pop(0)
        # import pdb
        # pdb.set_trace()
        running = seq(trials).filter(in_state(Trial.RUNNING))
        if running.empty():
            return None
        # Kill random ip
        ips = running.flat_map(lambda t: [t.config.get("head_location"), *t.config.get("worker_locations")]).set()
        return np.random.choice(list(ips))

class CloudExecutor(RayTrialExecutor):
    """Adds random failure injection to the TrialExecutor"""
    def __init__(self, deadline_s, queue_trials=True, reuse_actors=False, hooks=True, failure_injector=None):
        super(CloudExecutor, self).__init__(queue_trials=queue_trials, reuse_actors=reuse_actors)
        self._deadline_s = deadline_s
        self._start = time.time()
        #self.aggregates = [] is not hooks else None
        #self._hooks = hooks
        self._failure_injector = failure_injector

    #def _start_trial(self, trial, checkpoint=None, runner=None, train=True):

    def on_step_begin(self, trial_runner):
        """Before step() called, update the available resources"""
        # import pdb
        # pdb.set_trace()
        self._update_avail_resources()
        scheduler = trial_runner.scheduler_alg
        #logger.info("Scheduler timestamp {}".format(scheduler.timestamp))
        #if self._failure_injector and scheduler.timestamp:
        if self._failure_injector:
            targets = self._failure_injector.target(trial_runner.get_trials())

            if targets:
                if not isinstance(targets, list):
                    targets = [targets]
                for ip in targets:
                    logger.warning(f"Injecting failure at {ip}")
                    self._failure_injector.inject(ip, scheduler.provisioner)





def initialization_hook():
    # Need this for avoiding a connection restart issue on AWS.
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"

    # set the below if needed
    # print("NCCL DEBUG SET")
    # os.environ["NCCL_DEBUG"] = "INFO"


def cifar_creator(config):
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])  # meanstd transformation

    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])
    train_dataset = CIFAR10(
        root="~/data", train=True, download=True, transform=transform_train)
    validation_dataset = CIFAR10(
        root="~/data", train=False, download=False, transform=transform_test)

    if config.get("test_mode"):
        train_dataset = Subset(train_dataset, list(range(64)))
        validation_dataset = Subset(validation_dataset, list(range(64)))

    train_loader = DataLoader(
        train_dataset, batch_size=config[BATCH_SIZE], num_workers=2)
    validation_loader = DataLoader(
        validation_dataset, batch_size=config[BATCH_SIZE], num_workers=2)
    return train_loader, validation_loader


def optimizer_creator(model, config):
    """Returns optimizer"""
    return torch.optim.SGD(
        model.parameters(),
        lr=config.get("lr", 0.1),
        momentum=config.get("momentum", 0.9))

ray.init(address="auto", log_to_driver=True)
num_training_workers = 3

TorchTrainable = TorchTrainer.as_trainable(
        model_creator=ResNet18,
        data_creator=cifar_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=nn.CrossEntropyLoss,
        initialization_hook=initialization_hook,
        num_workers=num_training_workers,
        config={
            BATCH_SIZE: 128 * num_training_workers,
        },
        use_gpu=True,
        use_fp16=False)

class NoFaultToleranceTrainable(TorchTrainable):
    def _train(self):
        train_stats = self.trainer.train(max_retries=0, profile=True)
        validation_stats = self.trainer.validate(profile=True)
        stats = merge_dicts(train_stats, validation_stats)
        return stats

pbt_scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="val_loss",
        mode="min",
        perturbation_interval=1,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: np.random.uniform(0.001, 1),
            # allow perturbations within this set of categorical values
            "momentum": [0.8, 0.9, 0.99],
        })

reporter = CLIReporter()
reporter.add_metric_column("val_loss", "loss")
reporter.add_metric_column("val_accuracy", "acc")

failure_injector = FailureInjector()
executor = CloudExecutor(deadline_s=120, failure_injector=failure_injector)

analysis = tune.run(
        NoFaultToleranceTrainable,
        num_samples=4,
        config={
            "lr": tune.choice([0.001, 0.01, 0.1]),
            "momentum": 0.8
        },
        max_failures=-1,  # used for fault tolerance
        checkpoint_freq=2,  # used for fault tolerance
        progress_reporter=reporter,
        scheduler=pbt_scheduler,
        trial_executor=executor)

print(analysis.get_best_config(metric="val_loss", mode="min"))