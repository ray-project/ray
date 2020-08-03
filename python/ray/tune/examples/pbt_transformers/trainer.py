import logging
import os
from typing import Dict, Optional, Tuple

from ray import tune

import transformers
from transformers.file_utils import is_torch_tpu_available
from transformers.trainer_utils import PREFIX_CHECKPOINT_DIR

import torch
from torch.utils.data import Dataset

import wandb

logger = logging.getLogger(__name__)
"""A Trainer class integrated with Tune.
The only changes to the original transformers.Trainer are:
    - Report eval metrics to Tune
    - Save state using Tune's checkpoint directories
"""


class TuneTransformerTrainer(transformers.Trainer):
    def __init__(self, *args, wandb_args=None, **kwargs):
        self.wandb_args = wandb_args
        super().__init__(*args, **kwargs)

    def get_optimizers(
            self, num_training_steps: int
    ) -> Tuple[torch.optim.Optimizer, torch.optim.lr_scheduler.LambdaLR]:
        self.current_optimizer, self.current_scheduler = super(
        ).get_optimizers(num_training_steps)
        return (self.current_optimizer, self.current_scheduler)

    def evaluate(self,
                 eval_dataset: Optional[Dataset] = None) -> Dict[str, float]:
        eval_dataloader = self.get_eval_dataloader(eval_dataset)
        output = self._prediction_loop(
            eval_dataloader, description="Evaluation")
        self._log(output.metrics)

        tune.report(**output.metrics)

        self.save_state()

        return output.metrics

    def save_state(self):
        with tune.checkpoint_dir(step=self.global_step) as checkpoint_dir:
            self.args.output_dir = checkpoint_dir
            # This is the directory name that Huggingface requires.
            output_dir = os.path.join(
                self.args.output_dir,
                f"{PREFIX_CHECKPOINT_DIR}-{self.global_step}")
            self.save_model(output_dir)
            if self.is_world_master():
                torch.save(self.current_optimizer.state_dict(),
                           os.path.join(output_dir, "optimizer.pt"))
                torch.save(self.current_scheduler.state_dict(),
                           os.path.join(output_dir, "scheduler.pt"))

    def _setup_wandb(self):
        if self.is_world_master() and self.wandb_args is not None:
            wandb.init(
                project=self.wandb_args["project_name"],
                name=self.wandb_args["run_name"],
                id=self.wandb_args["run_name"],
                dir=tune.get_trial_dir(),
                config=vars(self.args),
                reinit=True,
                allow_val_change=True,
                resume=self.wandb_args["run_name"])
            # keep track of model topology and gradients, unsupported on TPU
            if not is_torch_tpu_available(
            ) and self.wandb_args["watch"] != "false":
                wandb.watch(
                    self.model,
                    log=self.wandb_args["watch"],
                    log_freq=max(100, self.args.logging_steps))
