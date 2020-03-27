import asyncio
import time

from tqdm import tqdm

import ray
from ray.util.sgd.torch.constants import BATCH_LOGS_RATE_LIMIT


@ray.remote(num_cpus=0)
class _ReporterActor:
    def __init__(self):
        # we need the new_data field to allow sending back None as the legs
        self._logs = {"new_data": False, "data": None}
        self._setup = {"new_data": False, "data": None}

    def _send_setup(self, data):
        self._setup = {"new_data": True, "data": data}

    def _send_logs(self, data):
        self._logs = {"new_data": True, "data": data}

    def _read_logs(self):
        res = self._logs

        self._logs = {"new_data": False, "data": None}

        return res

    def _read_setup(self):
        res = self._setup

        self._setup = {"new_data": False, "data": None}

        return res


class TqdmReporter:
    def __init__(self, actor):
        self.actor = actor

        self.last_packet_time = 0

    def _send_setup(self, packet):
        ray.get(self.actor._send_setup.remote(packet))

    def _send_logs(self, packet):
        cur_time = time.monotonic()
        if cur_time - self.last_packet_time < BATCH_LOGS_RATE_LIMIT:
            return

        self.last_packet_time = cur_time
        ray.get(self.actor._send_logs.remote(packet))

    def on_epoch_begin(self, info, training_op):
        if training_op.world_rank != 0:
            return

        self.last_packet_time = 0

        self._send_setup({"loader_len": len(training_op.train_loader)})

    def on_batch_end(self, batch_info, metrics, training_op):
        if training_op.world_rank != 0:
            return

        pbar_metrics = {}
        if "train_loss" in metrics:
            pbar_metrics["loss"] = metrics["train_loss"]

        self._send_logs({
            "batch_idx": batch_info["batch_idx"],
            "pbar_metrics": pbar_metrics
        })


class TqdmHandler:
    def __init__(self):
        self.batch_pbar = None
        self.reporter_actor = _ReporterActor.remote()

    def create_reporter(self):
        return TqdmReporter(self.reporter_actor)

    def handle_setup_packet(self, packet):
        n = self.num_steps
        if n is None:
            n = packet["loader_len"]

        desc = ""
        if self.train_info is not None and "epoch_idx" in self.train_info:
            if "num_epochs" in self.train_info:
                desc = "{}/{}e".format(self.train_info["epoch_idx"] + 1,
                                       self.train_info["num_epochs"])
            else:
                desc = "{}e".format(self.train_info["epoch_idx"] + 1)

        self.batch_pbar = tqdm(total=n, desc=desc, unit="batch", leave=False)

    def handle_logs_packet(self, packet):
        self.batch_pbar.n = packet["batch_idx"] + 1
        self.batch_pbar.set_postfix(packet["pbar_metrics"])

    def record_train_info(self, info, num_steps):
        self.train_info = info
        self.num_steps = num_steps

    async def update(self):
        setup_read, logs_read = await asyncio.gather(
            self.reporter_actor._read_setup.remote(),
            self.reporter_actor._read_logs.remote())

        if setup_read["new_data"]:
            self.handle_setup_packet(setup_read["data"])
        if logs_read["new_data"]:
            self.handle_logs_packet(logs_read["data"])
