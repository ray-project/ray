# flake8: noqa
# TODO(rliaw): Include this in the docs.

# fmt: off
# __custom_trainer_begin__
import torch

from ray.train.trainer import BaseTrainer
from ray import tune


class MyPytorchTrainer(BaseTrainer):
    def setup(self):
        self.model = torch.nn.Linear(1, 1)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=0.1)

    def training_loop(self):
        # You can access any Trainer attributes directly in this method.
        # self.datasets["train"] has already been
        # preprocessed by self.preprocessor
        dataset = self.datasets["train"]

        torch_ds = dataset.to_torch(label_column="y")
        loss_fn = torch.nn.MSELoss()

        for epoch_idx in range(10):
            loss = 0
            num_batches = 0
            for X, y in iter(torch_ds):
                # Compute prediction error
                pred = self.model(X.float())
                batch_loss = loss_fn(pred, y.float())

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

train_dataset = ray.data.from_items([{"x": i, "y": i} for i in range(3)])
my_trainer = MyPytorchTrainer(datasets={"train": train_dataset})
result = my_trainer.fit()
# __custom_trainer_usage_end__
# fmt: on
