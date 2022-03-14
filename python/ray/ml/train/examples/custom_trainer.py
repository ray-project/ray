# flake8: noqa
# TODO(amog): Add this to CI once Trainer has been implemented.
# TODO(rliaw): Include this in the docs.

# fmt: off
# __custom_trainer_begin__
import torch

from ray.ml.trainer import Trainer
from ray import tune


class MyPytorchTrainer(Trainer):
    def setup(self):
        self.model = torch.nn.Linear(1, 1)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=0.1)

    def training_loop(self):
        # You can access any Trainer attributes directly in this method.
        # self.train_dataset has already been preprocessed by self.preprocessor
        dataset = self.train_dataset

        torch_ds = dataset.to_torch()

        for epoch_idx in range(10):
            loss = 0
            num_batches = 0
            for X, y in iter(torch_ds):
                # Compute prediction error
                pred = self.model(X)
                batch_loss = torch.nn.MSELoss(pred, y)

                # Backpropagation
                self.optimizer.zero_grad()
                batch_loss.backward()
                self.optimizer.step()

                loss += batch_loss.item()
                num_batches += 1
            loss /= num_batches

            # Use Tune functions to report intermediate
            # results.
            tune.report(loss=loss, epoch=epoch_idx)


# __custom_trainer_end__
# fmt: on


# fmt: off
# __custom_trainer_usage_begin__
import ray

train_dataset = ray.data.from_items([1, 2, 3])
my_trainer = MyPytorchTrainer(train_dataset=train_dataset)
result = my_trainer.fit()
# __custom_trainer_usage_end__
# fmt: on
