import inspect
import logging

import torch
from pytorch_lightning.core.step_result import Result
from pytorch_lightning.overrides.data_parallel import \
    LightningDistributedDataParallel
from pytorch_lightning.utilities.model_utils import is_overridden
from pytorch_lightning.trainer.model_hooks import TrainerModelHooksMixin
from pytorch_lightning.trainer.optimizers import TrainerOptimizersMixin
import pytorch_lightning as ptl
from pytorch_lightning.utilities.exceptions import MisconfigurationException
from pytorch_lightning.utilities.memory import recursive_detach
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd.torch.constants import NUM_STEPS, SCHEDULER_STEP_BATCH, \
    SCHEDULER_STEP_EPOCH
from ray.util.sgd.utils import AverageMeterCollection, NUM_SAMPLES

tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass

logger = logging.getLogger(__name__)


class LightningOperator(TrainingOperator, TrainerModelHooksMixin,
                        TrainerOptimizersMixin):
    """A subclass of TrainingOperator created from a PTL ``LightningModule``.

    This class is returned by `TrainingOperator.from_ptl` and it's training
    state is defined by the Pytorch Lightning ``LightningModule`` that is
    passed into `from_ptl`. Training and validation functionality have
    already been implemented according to
    Pytorch Lightning's Trainer. But if you need to modify training,
    you should subclass this class and override the appropriate methods
    before passing in the subclass to `TorchTrainer`.

    .. code-block:: python

            MyLightningOperator = TrainingOperator.from_ptl(
                MyLightningModule)
            trainer = TorchTrainer(training_operator_cls=MyLightningOperator,
                ...)
    """

    def _configure_amp(self, amp, models, optimizers, apex_args=None):
        assert len(models) == 1
        model = models[0]
        assert isinstance(model, ptl.LightningModule)
        model, optimizers = model.configure_apex(
            amp, model, optimizers, amp_level="O2")
        return [model], optimizers

    def _configure_ddp(self, models, device_ids, ddp_args=None):
        assert len(models) == 1
        model = models[0]
        assert isinstance(model, ptl.LightningModule)
        model = LightningDistributedDataParallel(
            model, device_ids=device_ids, find_unused_parameters=True)
        return [model]

    @property
    def model(self):
        """The LightningModule to use for training.

        The returned model is wrapped in DDP if using distributed training.
        """
        return self._model

    @property
    def scheduler_dicts(self):
        """Returns list of scheduler dictionaries.

        List is empty if no schedulers are returned in the
        configure_optimizers method of your LightningModule.

        Default configuration is used if configure_optimizers
        returns scheduler objects.

        See
        https://pytorch-lightning.readthedocs.io/en/latest/lightning_module.html#configure-optimizers
        """
        return self._scheduler_dicts

    @property
    def optimizers(self):
        """Returns list of optimizers as returned by configure_optimizers."""
        return self._optimizers

    @property
    def schedulers(self):
        """Returns list of schedulers as returned by configure_optimizers.

        List is empty if no schedulers are returned in configure_optimizers.
        """
        return self._schedulers

    def get_model(self):
        """Returns original LightningModule, not wrapped in DDP."""
        if isinstance(self.model, LightningDistributedDataParallel):
            return self.model.module
        else:
            return self.model

    def setup(self, config):
        # Pass in config if ptl_module accepts it.
        ptl_class = self.__class__._lightning_module_cls
        if not issubclass(ptl_class, ptl.LightningModule):
            raise TypeError("Argument must be subclass of "
                            "pytorch_lightning.LightningModule. Got class {} "
                            "instead.".format(ptl_class))
        if "config" in inspect.signature(ptl_class.__init__).parameters:
            ptl_module = ptl_class(config=config)
        else:
            ptl_module = ptl_class()

        # This is needed for LightningDistributedDataParallel.
        ptl_module.testing = False

        # Call on_fit_start on instantiation.
        if self.is_function_implemented("on_fit_start", ptl_module):
            ptl_module.on_fit_start()

        # Only run data preparation once per node.
        if self.local_rank == 0 and self.is_function_implemented(
                "prepare_data", ptl_module):
            ptl_module.prepare_data()

        # Call model.setup.
        ptl_module.setup("fit")

        if not is_overridden("configure_optimizers", ptl_module):
            raise MisconfigurationException(
                "No `configure_optimizers()` method defined.")

        optimizers, self._scheduler_dicts, optimizer_frequencies = \
            self.init_optimizers(model=ptl_module)

        if len(optimizer_frequencies) > 0:
            logger.warning("Optimizer frequencies will be ignored. When "
                           "passing in multiple optimizers, you should "
                           "implement your own custom training loop.")

        lr_schedulers = []
        for scheduler in self.scheduler_dicts:
            if isinstance(scheduler, dict):
                # A scheduler dictionary is passed in.
                if "reduce_on_plateau" in scheduler and "monitor" in \
                        scheduler and scheduler["reduce_on_plateau"] is True:
                    logger.info(
                        "reduce_on_plateau and monitor will be "
                        "ignored "
                        "from the scheduler dict {}. To update a "
                        "ReduceLROnPlateau scheduler, you should use "
                        "TorchTrainer.update_schedulers.".format(scheduler))
                if "frequency" in scheduler and scheduler["frequency"] > 1:
                    logger.info("frequency will be ignored from the "
                                "scheduler dict {}.".format(scheduler))
                lr_schedulers.append(scheduler["scheduler"])
            else:
                lr_schedulers.append(scheduler)

        # Set this so register doesn't complain.
        self._scheduler_step_freq = "ptl"
        ddp_model, self._optimizers, self._schedulers = self.register(
            models=[ptl_module],
            optimizers=optimizers,
            schedulers=lr_schedulers)

        assert len(ddp_model) == 1
        self._model = ddp_model[0]

        model = self.get_model()
        if self.is_function_implemented("on_pretrain_routine_start", model):
            model.on_pretrain_routine_start()

        train_data_loader = None
        if self.__class__._train_dataloader:
            train_data_loader = self.__class__._train_dataloader
        elif self.is_function_implemented("train_dataloader", model):
            train_data_loader = model.train_dataloader()

        val_data_loader = None
        if self.__class__._val_dataloader:
            val_data_loader = self.__class__._val_dataloader
        elif self.is_function_implemented("val_dataloader", model):
            val_data_loader = model.val_dataloader()

        self.register_data(
            train_loader=train_data_loader, validation_loader=val_data_loader)

    def train_epoch(self, iterator, info):
        model = self.get_model()

        # Enable train mode.
        self.model.train()

        # Enable gradients.
        torch.set_grad_enabled(True)

        if self.is_function_implemented("on_train_epoch_start", model):
            model.on_train_epoch_start()

        if self.use_tqdm and self.world_rank == 0:
            desc = ""
            if info is not None and "epoch_idx" in info:
                if "num_epochs" in info:
                    desc = f"{info['epoch_idx'] + 1}/{info['num_epochs']}e"
                else:
                    desc = f"{info['epoch_idx'] + 1}e"

            # TODO: Implement len for Dataset?
            total = info[NUM_STEPS]
            if total is None:
                if hasattr(iterator, "__len__"):
                    total = len(iterator)

            _progress_bar = tqdm(
                total=total, desc=desc, unit="batch", leave=False)

        # Output for each batch.
        epoch_outputs = []

        for batch_idx, batch in enumerate(iterator):
            batch_info = {
                "batch_idx": batch_idx,
                "global_step": self.global_step
            }
            batch_info.update(info)
            batch_output = self.train_batch(batch, batch_info=batch_info)
            # batch output for each optimizer.
            epoch_outputs.append(batch_output)

            should_stop = batch_output["signal"] == -1

            if self.use_tqdm and self.world_rank == 0:
                _progress_bar.n = batch_idx + 1
                postfix = {}
                if "training_loss" in batch_output:
                    postfix.update(loss=batch_output["training_loss"])
                _progress_bar.set_postfix(postfix)

            for s_dict, scheduler in zip(self.scheduler_dicts,
                                         self.schedulers):
                if s_dict["interval"] == SCHEDULER_STEP_BATCH:
                    scheduler.step()

            self.global_step += 1

            if should_stop:
                break

        processed_outputs = None
        if is_overridden("training_epoch_end", model):
            raw_outputs = [eo["raw_output"] for eo in epoch_outputs]
            processed_outputs = model.training_epoch_end(raw_outputs)

        if processed_outputs is not None:
            if isinstance(processed_outputs, torch.Tensor):
                return_output = {"train_loss": processed_outputs}
            elif isinstance(processed_outputs, Result):
                raise ValueError("Result objects are not supported. Please "
                                 "return a dictionary instead.")
            elif isinstance(processed_outputs, dict):
                return_output = processed_outputs
            else:
                raise TypeError("training_epoch_end returned an invalid "
                                "type. It must return a Tensor, Result, "
                                "or dict.")
        else:
            # User did not override training_epoch_end
            assert isinstance(epoch_outputs, list)
            # Use AverageMeterCollection util to reduce results.
            meter_collection = AverageMeterCollection()
            for o in epoch_outputs:
                num_samples = o.pop(NUM_SAMPLES, 1)
                raw_output = o["raw_output"]
                if isinstance(raw_output, dict):
                    meter_collection.update(raw_output, num_samples)
                elif isinstance(raw_output, torch.Tensor):
                    meter_collection.update({
                        "train_loss": o["training_loss"]
                    }, num_samples)
                return_output = meter_collection.summary()

        if self.is_function_implemented("on_train_epoch_end", model):
            model.on_train_epoch_end(
                [eo.get("raw_output") for eo in epoch_outputs])

        for s_dict, scheduler in zip(self.scheduler_dicts, self.schedulers):
            if s_dict["interval"] == SCHEDULER_STEP_EPOCH:
                scheduler.step()

        return return_output

    def train_batch(self, batch, batch_info):
        # Get the original PTL module.
        model = self.get_model()
        optimizer = self.optimizers[0]
        batch_idx = batch_info["batch_idx"]
        epoch_idx = batch_info["epoch_idx"]

        if self.is_function_implemented("on_train_batch_start", model):
            response = model.on_train_batch_start(
                batch=batch, batch_idx=batch_idx, dataloader_idx=0)
            # Skip remainder of epoch if response is -1.
            if response == -1:
                return {"signal": -1}

        args = [batch, batch_idx]
        if len(self.optimizers) > 1:
            if self.has_arg("training_step", "optimizer_idx"):
                args.append(0)

        with self.timers.record("fwd"):
            if self._is_distributed:
                # Use the DDP wrapped model (self.model).
                output = self.model(*args)
            elif self.use_gpu:
                # Using single GPU.
                # Don't copy the batch since there is a single gpu that
                # the batch could be referenced from and if there are
                # multiple optimizers the batch will wind up copying it to
                # the same device repeatedly.
                device = self.device
                batch = model.transfer_batch_to_device(batch, device=device)
                args[0] = batch
                output = model.training_step(*args)
            else:
                # Using CPU.
                output = model.training_step(*args)

        if isinstance(output, Result):
            raise ValueError("TrainResult objects are not supported. Please "
                             "return a dictionary instead.")

        # allow any mode to define training_step_end
        # do something will all the dp outputs (like softmax)
        if is_overridden("training_step_end", model):
            output = model.training_step_end(output)

        # Extract loss from output if dictionary.
        try:
            loss = output["loss"]
        except Exception:
            if isinstance(output, torch.Tensor):
                loss = output
            else:
                raise RuntimeError(
                    "No `loss` value in the dictionary returned from "
                    "`model.training_step()`.")

        # If output contains tensors, detach them all.
        if isinstance(output, torch.Tensor):
            output = output.detach()
        elif isinstance(output, dict):
            output = recursive_detach(output)
        else:
            raise TypeError("training_step returned invalid type. It must "
                            "return either a Tensor, Result, or dict.")

        untouched_loss = loss.detach().clone()

        with self.timers.record("grad"):
            if self.use_fp16:
                with self._amp.scale_loss(loss, optimizer) as scaled_loss:
                    model.backward(scaled_loss, optimizer, optimizer_idx=0)
            else:
                model.backward(loss, optimizer, optimizer_idx=0)

        if self.is_function_implemented("on_after_backward", model):
            model.on_after_backward()

        with self.timers.record("apply"):
            optimizer.step()

        model.on_before_zero_grad(optimizer)

        model.optimizer_zero_grad(
            epoch=epoch_idx,
            batch_idx=batch_idx,
            optimizer=optimizer,
            optimizer_idx=0)

        if self.is_function_implemented("on_train_batch_end", model):
            model.on_train_batch_end(
                outputs=output,
                batch=batch,
                batch_idx=batch_idx,
                dataloader_idx=0)

        return {
            "signal": 0,
            "training_loss": untouched_loss.item(),
            "raw_output": output,
            # NUM_SAMPLES: len(batch)
        }

    def validate(self, val_iterator, info):
        self.model.zero_grad()
        self.model.eval()

        torch.set_grad_enabled(False)

        model = self.get_model()
        if self.is_function_implemented("on_validation_epoch_start", model):
            model.on_validation_epoch_start()

        val_outputs = []
        for batch_idx, batch in enumerate(val_iterator):
            batch_info = {"batch_idx": batch_idx}
            batch_info.update(info)
            batch_output = self.validate_batch(batch, batch_info)
            if batch_output is not None:
                val_outputs.append(batch_output)

        processed_outputs = None
        if is_overridden("validation_epoch_end", model):
            raw_outputs = [vo["raw_output"] for vo in val_outputs]
            processed_outputs = model.training_epoch_end(raw_outputs)

        if processed_outputs is not None:
            if isinstance(processed_outputs, torch.Tensor):
                return_output = {"val_loss": processed_outputs}
            elif isinstance(processed_outputs, Result):
                raise ValueError("Result objects are not supported. Please "
                                 "return a dictionary instead.")
            elif isinstance(processed_outputs, dict):
                return_output = processed_outputs
            else:
                raise TypeError("validation_epoch_end returned an invalid "
                                "type. It must return a Tensor, Result, "
                                "or dict.")
        else:
            # User did not override training_epoch_end
            assert isinstance(val_outputs, list)
            # Use AverageMeterCollection util to reduce results.
            meter_collection = AverageMeterCollection()
            for v in val_outputs:
                num_samples = v.pop(NUM_SAMPLES, 1)
                raw_output = v["raw_output"]
                if isinstance(raw_output, dict):
                    meter_collection.update(raw_output, num_samples)
                elif isinstance(raw_output, torch.Tensor):
                    meter_collection.update({
                        "val_loss": raw_output.item()
                    }, num_samples)
                return_output = meter_collection.summary()

        if self.is_function_implemented("on_validation_epoch_end", model):
            model.on_validation_epoch_end()

        # Set back to True so training will work.
        torch.set_grad_enabled(True)

        return return_output

    def validate_batch(self, batch, batch_info):
        model = self.get_model()
        batch_idx = batch_info["batch_idx"]
        if is_overridden("on_validation_batch_start", model):
            model.on_validation_batch_start(
                batch=batch, batch_idx=batch_idx, dataloader_idx=0)
        args = [batch, batch_idx]
        with self.timers.record("eval_fwd"):
            if self._is_distributed:
                # Use the DDP wrapped model (self.model).
                output = self.model(*args)
            elif self.use_gpu:
                # Using single GPU.
                device = self.device
                batch = model.transfer_batch_to_device(batch, device=device)
                args[0] = batch
                output = model.validation_step(*args)
            else:
                # Using CPU.
                output = model.validation_step(*args)

        if isinstance(output, Result):
            raise ValueError("EvalResult objects are not supported. Please "
                             "return a dictionary instead.")

        if is_overridden("on_validation_step_end", model):
            output = model.validation_step_end(output)

        if self.is_function_implemented("on_validation_batch_end", model):
            model.on_validation_batch_end(
                outputs=output,
                batch=batch,
                batch_idx=batch_idx,
                dataloader_idx=0)
        return {
            "raw_output": output,
            # NUM_SAMPLES: len(batch)
        }

    def state_dict(self):
        state_dict = {}
        self.get_model().on_save_checkpoint(checkpoint=state_dict)
        return state_dict

    def load_state_dict(self, state_dict):
        self.get_model().on_load_checkpoint(checkpoint=state_dict)

    def _get_train_loader(self):
        if not hasattr(self, "_train_loader") or \
                self._train_loader is None:
            raise RuntimeError("Training Operator does not have any "
                               "registered training loader. Make sure "
                               "to pass in a training loader to "
                               "TrainingOperator.from_ptl or implement "
                               "train_dataloader in your LightningModule.")
        return self._train_loader

    def _get_validation_loader(self):
        if not hasattr(self, "_validation_loader") or \
                self._validation_loader is None:
            raise RuntimeError("Training Operator does not have any "
                               "registered validation loader. Make sure "
                               "to pass in a validation loader to "
                               "TrainingOperator.from_ptl or implement "
                               "val_dataloader in your LightningModule.")
        return self._validation_loader
