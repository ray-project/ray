from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import math
import time
import sys
import logging
import tempfile
import torch
import torchvision
import os
import random
import pandas as pd
import numpy as np
import ray

# TODO: Take this out of RLlib?
from ray.rllib.utils import TimerStat
from ray.tune import Trainable

import ray

import torch.nn as nn
import torch.nn.parallel
import torch.optim
import torch.utils.data
import torch.utils.data.distributed
import torchvision.transforms as transforms
import torchvision.datasets as datasets
import torchvision.models as models

from ray.experimental.torch.helpers import (train, adjust_learning_rate,
                                            validate)

DEFAULT_CONFIG = {
    # Arguments to pass to the optimizer
    "starting_lr": 0.1,
    "weight_decay": 5e-4,
    # Pins actors to cores
    "pin": False,
    "model_creator": None,
    "batch_per_device": 64,
    "max_batch_size": 1024,
    # "num_workers": 2,
    "devices_per_worker": 1,
    "primary_resource": "extra_gpu",
    "gpu": False,
    "verbose": False
}

logger = logging.getLogger(__name__)


class PyTorchRunner(object):
    def __init__(self,
                 batch_size,
                 starting_lr=0.1,
                 weight_decay=5e-4,
                 verbose=False):
        import torch.backends.cudnn as cudnn
        cudnn.benchmark = True
        self.batch_size = batch_size
        self.epoch = 0
        self.starting_lr = starting_lr
        self.weight_decay = weight_decay
        self.verbose = verbose
        self._setup_timers = {
            "setup_proc": TimerStat(),
            "setup_model": TimerStat(),
            "get_state": TimerStat(),
            "set_state": TimerStat()
        }

    def node_state(self):
        return {
            "node_ip": ray.services.get_node_ip_address(),
            "gpu_id": int(os.environ["CUDA_VISIBLE_DEVICES"])
        }

    def setup_proc_group(self, dist_url, world_rank, world_size):
        # self.try_stop()
        os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
        import torch.distributed as dist
        with self._setup_timers["setup_proc"]:
            self.world_rank = world_rank
            if self.verbose:
                print(
                    f"Inputs to process group: dist_url: {dist_url} world_rank: {world_rank} world_size: {world_size}"
                )

            # Turns out NCCL is TERRIBLE, do not USE - does not release resources
            dist.init_process_group(
                backend="gloo",
                init_method=dist_url,
                rank=world_rank,
                world_size=world_size)

            original_cuda = os.environ["CUDA_VISIBLE_DEVICES"]
            self.local_rank = int(original_cuda)
            ########## This is a hack because set_devices fails otherwise #################
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
                [str(i) for i in range(ray.services._autodetect_num_gpus())])
            ################################################################################

            torch.cuda.set_device(self.local_rank)
            if self.verbose:
                print("Device Set.")

            # dist.barrier()
            # if self.verbose:
            #     print("Barrier Passed.")
            # torch.distributed.barrier()

    def setup_model(self):
        with self._setup_timers["setup_model"]:
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
            from filelock import FileLock
            with FileLock("./data.lock"):
                trainset = torchvision.datasets.CIFAR10(
                    root='./data',
                    train=True,
                    download=True,
                    transform=transform_train)
            valset = torchvision.datasets.CIFAR10(
                root='./data',
                train=False,
                download=False,
                transform=transform_test)
            num_classes = 10

            # Create DistributedSampler to handle distributing the dataset across nodes when training
            # This can only be called after torch.distributed.init_process_group is called
            logger.info("Creating distributed sampler")
            self.train_sampler = torch.utils.data.distributed.DistributedSampler(
                trainset)

            # Create the Dataloaders to feed data to the training and validation steps
            self.train_loader = torch.utils.data.DataLoader(
                trainset,
                batch_size=self.batch_size,
                shuffle=(self.train_sampler is None),
                num_workers=4,
                pin_memory=False,
                sampler=self.train_sampler)
            self.val_loader = torch.utils.data.DataLoader(
                valset,
                batch_size=self.batch_size,
                shuffle=False,
                num_workers=4,
                pin_memory=False)

            self.model = models.resnet18(pretrained=False).cuda()
            # Make model DistributedDataParallel
            logger.info("Creating DDP Model")
            self.model = torch.nn.parallel.DistributedDataParallel(
                self.model,
                device_ids=[self.local_rank],
                output_device=self.local_rank)

            # define loss function (criterion) and optimizer
            self.criterion = nn.CrossEntropyLoss().cuda()
            self.optimizer = torch.optim.SGD(
                self.model.parameters(),
                lr=self.starting_lr,
                momentum=0.9,
                weight_decay=self.weight_decay)

        # os.environ["CUDA_VISIBLE_DEVICES"] = original_cuda

    def step(self):
        # Set epoch count for DistributedSampler
        self.train_sampler.set_epoch(self.epoch)

        # Adjust learning rate according to schedule
        adjust_learning_rate(self.starting_lr, self.optimizer, self.epoch)

        # train for one self.epoch

        if self.verbose:
            print("\nBegin Training Epoch {}".format(self.epoch + 1))
        train_stats = train(self.train_loader, self.model, self.criterion,
                            self.optimizer, self.epoch)

        # evaluate on validation set

        if self.verbose:
            print("Begin Validation @ Epoch {}".format(self.epoch + 1))
        val_stats = validate(self.val_loader, self.model, self.criterion)
        self.epoch += 1
        train_stats.update(val_stats)
        train_stats.update(self.stats())

        if self.verbose:
            print({k: type(v) for k, v in train_stats.items()})
        return train_stats

    def stats(self):
        stats = {
            k + "_time_mean": t.mean
            for k, t in self._setup_timers.items()
        }
        stats.update(
            {k + "_time_total": t.sum
             for k, t in self._setup_timers.items()})
        return stats

    def get_state(self, ckpt_path):
        with self._setup_timers["get_state"]:
            try:
                os.makedirs(ckpt_path)
            except OSError:
                logger.exception("failed making dirs")
            if self.verbose:
                print("getting state")
            state_dict = {}
            tmp_path = os.path.join(ckpt_path,
                                    ".state{}".format(self.world_rank))
            torch.save({
                "model": self.model.state_dict(),
                "opt": self.optimizer.state_dict()
            }, tmp_path)

            with open(tmp_path, "rb") as f:
                state_dict["model_state"] = f.read()

            os.unlink(tmp_path)
            state_dict["epoch"] = self.epoch
            if self.verbose:
                print("Got state.")

        return state_dict

    def set_state(self, state_dict, ckpt_path):
        with self._setup_timers["set_state"]:
            if self.verbose:
                print("setting state for {}".format(self.world_rank))
            try:
                os.makedirs(ckpt_path)
            except OSError:
                print("failed making dirs")
            tmp_path = os.path.join(ckpt_path,
                                    ".state{}".format(self.world_rank))

            with open(tmp_path, "wb") as f:
                f.write(state_dict["model_state"])

            checkpoint = torch.load(tmp_path)
            self.model.load_state_dict(checkpoint["model"])
            self.optimizer.load_state_dict(checkpoint["opt"])

            os.unlink(tmp_path)
            # self.model.train()
            self.epoch = state_dict["epoch"]
            if self.verbose:
                print("Loaded state.")

    def try_stop(self):
        try:
            import torch.distributed as dist
            dist.destroy_process_group()
        except Exception:
            logger.exception("Stop failed.")


class PytorchSGD(Trainable):
    ADDRESS_TMPL = "tcp://{ip}:{port}"

    def _setup(self, config):
        # model_creator = config["model_creator"]
        # devices_per_worker = self.config["devices_per_worker"]
        primary_resource = self.resources._asdict()[self.config[
            "primary_resource"]]
        self._next_iteration_start = time.time()
        self._time_so_far = 0

        self.config = config or DEFAULT_CONFIG

        RemotePyTorchRunner = ray.remote(num_gpus=1)(PyTorchRunner)
        # assert primary_resource >= devices_per_worker, (
        #     "Unable to provide enough resources - {} >= {}!".format(
        #         primary_resource, devices_per_worker))
        print("Creating {} workers".format(int(primary_resource)))
        batch_per_device = min(
            int(config["max_batch_size"] / primary_resource),
            config["batch_per_device"])
        self.remote_workers = [
            RemotePyTorchRunner.remote(
                batch_per_device,
                starting_lr=config["starting_lr"],
                weight_decay=config["weight_decay"],
                verbose=config["verbose"])
            for i in range(int(primary_resource))
        ]
        # local_ranks = defaultdict(int)
        setup_futures = []
        master_ip = None
        port = int(4000 + random.choice(np.r_[:1000]))

        for world_rank, worker in enumerate(self.remote_workers):
            node_state = ray.get(worker.node_state.remote())
            if not master_ip:
                master_ip = node_state["node_ip"]
            setup_futures += [
                worker.setup_proc_group.remote(
                    self.ADDRESS_TMPL.format(ip=master_ip, port=port),
                    world_rank, len(self.remote_workers))
            ]
            # local_ranks[worker_ip] += 1

        ray.get(setup_futures)
        [worker.setup_model.remote() for worker in self.remote_workers]

        self.optimizer_timer = TimerStat(window_size=2)

    def _train(self):
        with self.optimizer_timer:
            worker_stats = ray.get(
                [w.step.remote() for w in self.remote_workers])
        # res = self._fetch_metrics_from_remote_workers()
        df = pd.DataFrame(worker_stats)
        results = df.mean().to_dict()
        # self.optimizer_timer.push_units_processed(res["total_samples_this_iter"])
        # res["sample_throughput"] = self.optimizer_timer.mean_throughput
        # # res.update(self.optimizer.stats())
        # res.update(self._evaluate())

        self._time_so_far += time.time() - self._next_iteration_start
        self._next_iteration_start = time.time()
        results.update(time_since_start=self._time_so_far)

        return results

    def _save(self, ckpt):
        return {
            "worker_state": ray.get(
                self.remote_workers[0].get_state.remote(ckpt)),
            "time_so_far": self._time_so_far,
            "ckpt_path": ckpt
        }

    def _restore(self, ckpt):
        self._time_so_far = ckpt["time_so_far"]
        self._next_iteration_start = time.time()
        worker_state = ray.put(ckpt["worker_state"])
        states = []

        for worker in self.remote_workers:
            states += [
                worker.set_state.remote(worker_state, ckpt["ckpt_path"])
            ]

        ray.get(states)

    def _stop(self):
        stops = []
        for worker in self.remote_workers:
            stops += [worker.try_stop.remote()]
            stops += [worker.__ray_terminate__.remote()]
