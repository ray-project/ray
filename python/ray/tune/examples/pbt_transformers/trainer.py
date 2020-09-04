import logging
import os
from typing import Dict, Optional, Tuple

from ray import tune

import transformers
from transformers.trainer_utils import PREFIX_CHECKPOINT_DIR

import torch
from torch.utils.data import Dataset

logger = logging.getLogger(__name__)
"""A Trainer class integrated with Tune.
The only changes to the original transformers.Trainer are:
    - Report eval metrics to Tune
    - Save state using Tune's checkpoint directories
"""


class TuneTransformerTrainer(transformers.Trainer):
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
        self.save_state()
        tune.report(**output.metrics)

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
