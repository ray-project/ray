import time
import argparse
import sys
import logging

from tqdm import tqdm, trange

from torch.utils.data import Subset

import ray
from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

logger = logging.getLogger("rqtorch")

class Namespace(dict):
    def __init__(self):
        pass

    def __getattr__(self, attr):
        if attr not in self:
            raise AttributeError()

        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value

_default_interval_names = ["log", "checkpoint", "backup"]
def _get_training_operator_cls(super_cls):
    class _TrainingOperator(super_cls):
        def setup(self, config):
            self.myconfig = config.training_op_config
            self.logger = self.myconfig.logging_cls(config)

            if self.world_rank == 0:
                self.batch_logs = Namespace()
                self.batch_logs.packet_type = "batch_logs"

            super().setup(self.myconfig.user_config)

        # todo: this is kind of ugly
        def batch_interval_log(self, interval):
            if hasattr(super(), "batch_interval_log"):
                return super().batch_interval_log(interval)

        # todo: this is kind of ugly
        def batch_log(self):
            if hasattr(super(), "batch_log"):
                return super().batch_log(interval)

            self.logger.log_object("Train loss", self.last_logs["loss"])

        # todo: this is kind of ugly
        def make_pbar_metrics(self):
            res = {
                k: self.last_logs[k] for k in ["loss"]
            }
            if hasattr(super(), "make_pbar_metrics"):
                return super().make_pbar_metrics(res)
            return res

        def train_epoch(self, iterator, info):
            self.sysinfo = info["_system_training_op_info"]

            if self.world_rank == 0:
                tqdm_setup = Namespace()
                tqdm_setup.packet_type = "tqdm_setup"
                tqdm_setup.loader_len = len(self.train_loader)

                # todo: we need a proper way to wait here for the pbar to finish initing
                self.send_batch_logs(tqdm_setup)
                time.sleep(.4)

            return super().train_epoch(iterator, info)

        def train_batch(self, batch, batch_info):
            self.last_logs = super().train_batch(batch, batch_info)

            # todo: this only supports synchronous training
            if self.world_rank == 0:
                batch_idx = batch_info["batch_idx"]
                # todo: maybe do not recreate this function every time? is this expensive?
                def run_intervals(intervals_key, f):
                    for interval in self.sysinfo.intervals[intervals_key]:
                        unit, duration = interval

                        if unit != "b":
                            continue
                        if batch_idx - self.sysinfo.last_action_batches[intervals_key] < duration:
                            continue

                        f(interval)
                        self.sysinfo.last_action_batches[intervals_key] = batch_idx

                run_intervals("log", self.batch_interval_log)
                run_intervals("checkpoint", lambda interval: print("Checkpoint mock executed"))
                run_intervals("backup", lambda interval: print("Debug mock executed"))

                pbar_logs = self.make_pbar_metrics()

                self.batch_logs.batch_idx = batch_idx
                self.batch_logs.pbar_logs = pbar_logs

                self.send_batch_logs(self.batch_logs)

                self.batch_log()
                self.logger.commit_batch()

            return self.last_logs
    return _TrainingOperator

    def state_dict(self):
        super_state = super().state_dict()
        return {
            "super_state": super_state
        }

    def load_state_dict(self, state_dict):
        super().load_state_dict(state_dict["super_state"])

class System():
    def __init__(self):
        # custom config, can be modified by user to pass info to
        # the TorchTrainer
        self.config = Namespace()

        self.intervals = {}
        for k in _default_interval_names:
            self.intervals[k] = []


        self._arg_parser = argparse.ArgumentParser(allow_abbrev=False)
        self._arg_subparsers = self._arg_parser.add_subparsers(
            title="Mode",
            dest="mode",
            metavar="MODE",
            description="Action to execute.")
        self._add_train_subparser()
        self._add_eval_subparser()
        # self._add_infer_subparser()

        self._ray_params = {}
        self._trainer_params = {}

    def train(self, *args, **kwargs):
        last_action_times = {}
        last_action_epochs = {}

        if not self.args.restart:
            try:
                self.trainer.restore("checkpoint_last.pth")
            except FileNotFoundError:
                # todo: check checkpoint_best.pth
                logger.warning("Last checkpoint not found, restarting training.")
                pass
            # todo: load interval states

        # todo: add a debug flag for debugging backups too?
        # or debug when debugging checkpoints?
        if self.args.debug_checkpoint:
            logger.info("--debug-checkpoint is set, saving a checkpoint then quitting.")
            self.trainer.save("checkpoint_last.pth")
            return

        # todo: is this a large performance impact? if so switch
        # to non-monotonic
        cur_time = time.monotonic()
        for k in self.intervals:
            if not k in last_action_times:
                last_action_times[k] = cur_time
                # we run actions after every epochs, so the last epoch we ran the
                # action after was -1
                last_action_epochs[k] = -1

        epoch_start = cur_time
        iterator = trange(
            self.args.num_epochs,
            unit="epoch")

        for epoch_idx in iterator:
            # todo: load checkpoints from mid-epoch properly
            #       we actually don't checkpoint mid-epoch currently
            #       but might have to address this later
            training_op_info = Namespace()
            training_op_info.intervals = self.intervals
            training_op_info.args = self.args

            training_op_info.last_action_batches = {}
            for k in self.intervals:
                if not k in training_op_info.last_action_batches:
                    # we run actions after batches, so the last batch we ran the
                    # action after was -1
                    training_op_info.last_action_batches[k] = -1

            # todo: info is not user-space currently, fix this after it is
            info = dict(_system_training_op_info=training_op_info)
            if "info" in kwargs:
                info.update(kwargs["info"])
            kwargs["info"] = info


            batch_pbar = None
            def handle_head_packet(packet):
                nonlocal batch_pbar

                if packet.packet_type == "tqdm_setup":
                    num_steps = kwargs.get("num_steps", packet.loader_len)

                    unit = "batch"
                    if self.args.progress_bar_units == "batches":
                        unit = "batch"
                    # todo: verify that it's one of these
                    elif self.args.progress_bar_units == "samples":
                        unit = "sample"
                        num_steps *= self.args.total_batch_size

                    batch_pbar = tqdm(
                        total=num_steps,
                        desc="{}/{}e".format(epoch_idx+1, self.args.num_epochs),
                        unit=unit
                    )
                    return

                if packet.packet_type == "batch_logs":
                    if self.args.progress_bar_units == "batches":
                        batch_pbar.n = packet.batch_idx+1
                    elif self.args.progress_bar_units == "samples":
                        batch_pbar.n = packet.batch_idx * self.args.total_batch_size + 1
                    # batch_pbar.refresh() # will be needed if set_postfix
                                           # is ever removed
                    batch_pbar.set_postfix(packet.pbar_logs)


            kwargs["batch_logs_handler"] = handle_head_packet
            logs = self.trainer.train(*args, **kwargs)


            # todo: make this customizable
            pbar_logs = {
                "loss": logs["mean_train_loss"]
            }
            iterator.set_postfix(pbar_logs)


            epoch_end = time.monotonic()
            epoch_time = epoch_end - epoch_start

            # todo: maybe do not recreate this function every time? is this expensive?
            def run_intervals(intervals_key, f):
                for (unit, duration) in self.intervals[intervals_key]:
                    if unit == "s" and epoch_end - last_action_times[intervals_key] >= duration:
                        f()
                        last_action_times[intervals_key] = epoch_end
                        continue

                    if unit == "e" and i - last_action_epochs[intervals_key] >= duration:
                        f()
                        last_action_epochs[intervals_key] = i
                        continue

            # todo: make this customizable
            def log():
                print(logs)
            run_intervals("log", lambda: log)
            run_intervals("checkpoint", lambda: print("Checkpoint mock executed"))
            run_intervals("backup", lambda: print("Debug mock executed"))

            if self.args.debug_epoch:
                logger.info("--debug-epoch is set, quitting after one epoch.")
                break

        # todo: should we always checkpoint after training?
        self.trainer.save("checkpoint_last.pth")

    # todo: this has to be called
    def init_ray(self, **kwargs):
        params = self._ray_params.copy()
        params.update(kwargs)

        return ray.init(**params)

    def create_trainer(
            self,
            model_creator,
            data_creator,
            optimizer_creator,
            loss_creator,
            **kwargs):
        training_op_config = Namespace()

        from ml_logging import WandbLogger, Logger
        training_op_config.user_config = self.config
        training_op_config.logging_cls = Logger

        trainer_config = Namespace()
        trainer_config.user_config = self.config
        trainer_config.training_op_config = training_op_config

        params = self._trainer_params.copy()
        params["config"] = trainer_config
        params.update(kwargs)
        params["training_operator_cls"] = _get_training_operator_cls(kwargs.get("training_operator_cls", TrainingOperator))

        # todo: call this on dataset creation?
        if self.args.debug_batch:
            logger.info("--debug-batch is set, will subset the dataset to the first element only.")

        # todo: use subset in runner too?
        def possibly_truncated_data_creator(config):
            res = data_creator(config)
            # todo: some modes might not even have this option. will they even create a datset then though?
            #       watch use cases
            if self.args.debug_batch:
                res = Subset(res, [0])
            return res

        self.trainer = TorchTrainer(
            lambda c: model_creator(c.user_config),
            lambda c: possibly_truncated_data_creator(c.user_config),
            lambda model, c: optimizer_creator(model, c.user_config),
            lambda c: loss_creator(c.user_config), # fixme: this is not correct if user passes in a torch.nn loss class!!!! # see runner for details
            **params)
        return self.trainer

    def parse_args(self):
        self.args = self._arg_parser.parse_args()

        if self.args.mode is None:
            self._arg_parser.error("the following arguments are required: mode")

        self._ray_params["address"] = self.args.ray_address

        if self.args.mode == "train":
            self._trainer_params["num_replicas"] = self.args.number_of_workers
            # todo: we cannot require GPU for now, that would require changing
            # TorchTrainer (there is a todo)
            self._trainer_params["use_gpu"] = not self.args.no_gpu
            # will be divided by num of workers in trainer
            self._trainer_params["batch_size"] = self.args.total_batch_size
            self._trainer_params["use_fp16"] = self.args.mixed_precision

            def populate_intervals(interval_strs, intervals_key):
                intervals = self.intervals[intervals_key]
                for i_str in interval_strs:
                    try:
                        duration = float(i_str[:-1])
                    except ValueError:
                        raise ValueError("Interval value is not a valid number: {}".format(i_str[:-1]))
                    unit = i_str[-1:]

                    if unit not in ["s", "m", "h", "e", "b"]:
                        raise ValueError("Unknown interval {} unit: \"{}\"".format(intervals_key, unit))

                    if unit == "m":
                        unit = "s"
                        duration *= 60
                    if unit == "h":
                        unit = "s"
                        duration *= 60 * 60

                    if unit == "e" or unit == "b":
                        # todo: support mid-batch or mid-epoch logging?
                        # todo: should this be a warning instead?
                        if not duration.is_integer():
                            raise ValueError("Batch and epoch intervals must be integer")
                        duration = int(duration)
                        if duration <= 0:
                            raise ValueError("Batch and epoch inrevals must be at least 1")

                    intervals.append((unit, duration))

            populate_intervals(self.args.log_interval, "log")
            populate_intervals(self.args.checkpoint_interval, "checkpoint")
            populate_intervals(self.args.backup_interval, "backup")

        return self.args

    # todo: consider switching to click? somehow?
    def _add_default_args(self, p):
        p.add_argument(
            "--ray-address",
            metavar='ADDRESS',
            type=str,
            default="auto",
            help="Address of the Ray head node [default=auto].")
        p.add_argument(
            "-n",
            "--number-of-workers",
            type=int,
            default=1,
            help="Number of workers to run in parallel.")

        gpu_params = p.add_mutually_exclusive_group()
        gpu_params.add_argument(
            "--no-gpu",
            action="store_true",
            default=False,
            help="Do not use the GPU even if avaiable.")
        gpu_params.add_argument(
            "--require-gpu",
            action="store_true",
            default=False,
            help="Print an error if the GPU is not available.")

        # todo: implement somehow. we don't get a config though to pass
        # to model_creator
        #
        # if self.args.debug_summary:
        #     from torchsummary import summary
        #     summary(res, input_size=(3, 500, 500))
        p.add_argument(
            "--debug-summary",
            action="store_true",
            default=False,
            help="Print the model summary and quit for debugging purposes.")

    def _add_train_subparser(self):
        self._arg_train_subparser = self._arg_subparsers.add_parser("train", help="Train the model.")
        p = self._arg_train_subparser
        self._add_default_args(p)
        # todo: support tune
        # p.add_argument(
        #     "--tune",
        #     action="store_true",
        #     default=False,
        #     help="Use Tune for hyper-parameter search.")
        p.add_argument(
            "-d",
            "--debug-batch",
            action="store_true",
            default=False,
            help="Quit after a single batch for debugging purposes.")
        p.add_argument(
            "-D",
            "--debug-epoch",
            action="store_true",
            default=False,
            help="Quit after a single epoch for debugging purposes.")

        # todo: support batch size per worker? maybe, but as an advanced options
        p.add_argument(
            "-b",
            "--total-batch-size",
            type=int,
            default=32,
            help="Total batch size (divided evenly between each worker).")

        p.add_argument(
            "-e",
            "--num-epochs",
            type=int,
            required=True,
            help="Number of training epochs.")
        # todo: replace with automatic experiment versioning
        p.add_argument(
            "-r",
            "--restart",
            action="store_true",
            default=False,
            help="Restart training even if a checkpoint exists.")
        p.add_argument(
            "--mixed-precision",
            "--fp16",
            "--float16",
            action="store_true",
            default=False,
            help="Use mixed precision (16-bit + 32-bit floating point) training using apex.")
        p.add_argument(
            "--progress-bar-units",
            "--pbar-units",
            default="batches",
            help="What to measure the progress in for the progress bar (\"batches\" or \"samples\") [default=batches].")

        p.add_argument(
            "--log-interval",
            metavar='INTERVAL',
            type=str,
            default=["60s"],
            action="append",
            help="Time to wait between logging to console. Can be specified multiple times. Format: <number><unit>, where unit is \"h\" for hours, \"m\" for minutes, \"s\" for seconds, \"e\" for epochs, and \"b\" for minibatches.")
        # todo: fix the help message
        p.add_argument(
            "--checkpoint-interval",
            metavar='INTERVAL',
            type=str,
            default=["5m"], # we only keep latest and best so who cares
            action="append",
            help="Time to wait between saving checkpoints. Can be specified multiple times.")
        # todo: default too low for real training?
        p.add_argument(
            "--backup-interval",
            metavar='INTERVAL',
            type=str,
            default=["30m"],
            action="append",
            help="Time to wait between backing up the latest and best checkpoint. Can be specified multiple times.")


        p.add_argument(
            "--debug-checkpoint",
            action="store_true",
            default=False,
            help="Save the model checkpoint and quit for debugging purposes.")

    def _add_eval_subparser(self):
        self._arg_eval_subparser = self._arg_subparsers.add_parser("eval", help="Evaluate the model.")
        p = self._arg_eval_subparser
        self._add_default_args(p)
        p.add_argument(
            "-d",
            "--debug-batch",
            action="store_true",
            default=False,
            help="Run a single batch and quit for debugging purposes.")

        # todo: support log-interval?

    def _add_infer_subparser(self):
        self._arg_infer_subparser = self._arg_subparsers.add_parser("infer", help="Run inference using the model.")
        p = self._arg_infer_subparser
        self._add_default_args(p)

    def create_custom_subparser(self, *args, **kwargs):
        p = self._arg_subparsers.add_parser(*args, **kwargs)
        self._add_default_args(p)
        return p

    def add_custom_argument(self, *args, **kwargs):
        self._arg_parser.add_argument(*args, **kwargs)

    def add_custom_train_argument(self, *args, **kwargs):
        self._arg_train_subparser.add_argument(*args, **kwargs)

    def add_custom_eval_argument(self, *args, **kwargs):
        self._arg_eval_subparser.add_argument(*args, **kwargs)

    def add_custom_infer_argument(self, *args, **kwargs):
        self._arg_infer_subparser.add_argument(*args, **kwargs)
