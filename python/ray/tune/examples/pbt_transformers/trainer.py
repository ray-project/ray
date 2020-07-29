from typing import Optional

import logging
import os
from packaging import version
import torch
from torch.utils.data import Dataset
from torch.utils.data.dataloader import DataLoader
from torch.utils.data.distributed import DistributedSampler
import transformers
from ray import tune
from tqdm.auto import tqdm, trange
from typing import Dict, Optional, Tuple

from transformers.file_utils import is_apex_available, is_torch_tpu_available
from transformers.trainer_utils import TrainOutput, PREFIX_CHECKPOINT_DIR
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

    def get_optimizers(self, num_training_steps: int) -> Tuple[
        torch.optim.Optimizer, torch.optim.lr_scheduler.LambdaLR]:
        self.current_optimizer, self.current_scheduler = super().get_optimizers(num_training_steps)
        return (self.current_optimizer, self.current_scheduler)

    def evaluate(self, eval_dataset: Optional[Dataset] = None) -> Dict[str, float]:
        eval_dataloader = self.get_eval_dataloader(eval_dataset)
        output = self._prediction_loop(eval_dataloader, description="Evaluation")
        self._log(output.metrics)

        tune.report(**output.metrics)

        self.save_state()

        return output.metrics

    def save_state(self):
        self.args.output_dir = tune.make_checkpoint_dir()
        output_dir = os.path.join(self.args.output_dir, f"{PREFIX_CHECKPOINT_DIR}-{self.global_step}")
        self.save_model(output_dir)
        if self.is_world_master():
            torch.save(self.current_optimizer.state_dict(), os.path.join(output_dir, "optimizer.pt"))
            torch.save(self.current_scheduler.state_dict(), os.path.join(output_dir, "scheduler.pt"))
        tune.save_checkpoint(output_dir)

    def _setup_wandb(self):
        if self.is_world_master() and self.wandb_args is not None:
            wandb.init(project=self.wandb_args["project_name"], name=self.wandb_args["run_name"],
                       id=self.wandb_args["run_name"], config=vars(self.args))
            # wandb.init(project=os.getenv("WANDB_PROJECT", "huggingface"), config=vars(self.args))
#             # keep track of model topology and gradients, unsupported on TPU
#             if not is_torch_tpu_available() and self.wandb_args["watch"] != "false":
#                 wandb.watch(
#                     self.model, log=self.wandb_args["watch"], log_freq=max(100, self.args.logging_steps)
#                 )

