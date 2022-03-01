import torch
import torch.nn.functional as F
from torch import nn
from torch.utils.data import DataLoader

import ray
import ray.util.data as ml_data
import ray.util.iter as parallel_it
from ray.util.sgd.torch.torch_dataset import TorchMLDataset
from ray.util.sgd.torch.torch_trainer import TorchTrainer
from ray.util.sgd.torch.training_operator import TrainingOperator


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(1, 128)
        self.fc2 = nn.Linear(128, 1)

    def forward(self, x):
        x = self.fc1(x)
        x = F.relu(x)
        x = self.fc2(x)
        return x


def make_train_operator(ds: TorchMLDataset):
    class IdentityTrainOperator(TrainingOperator):
        def setup(self, config):
            model = Net()
            optimizer = torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))
            loss = torch.nn.MSELoss()

            batch_size = config["batch_size"]
            train_data = ds.get_shard(
                self.world_rank, shuffle=True, shuffle_buffer_size=4
            )
            train_loader = DataLoader(train_data, batch_size=batch_size)

            self.model, self.optimizer, self.criterion = self.register(
                models=model, optimizers=optimizer, criterion=loss
            )

            self.register_data(train_loader=train_loader, validation_loader=None)

    return IdentityTrainOperator


def main():
    num_points = 32 * 100 * 2
    data = [i * (1 / num_points) for i in range(num_points)]
    it = parallel_it.from_items(data, 2, False).for_each(lambda x: [x, x])
    # this will create MLDataset with column RangeIndex(range(2))
    ds = ml_data.from_parallel_iter(it, True, batch_size=32, repeated=False)
    torch_ds = ds.to_torch(feature_columns=[0], label_column=1)

    trainer = TorchTrainer(
        num_workers=2,
        training_operator_cls=make_train_operator(torch_ds),
        add_dist_sampler=False,
        config={"batch_size": 32},
    )
    for i in range(10):
        trainer.train(num_steps=100)
        model = trainer.get_model()
        print("f(0.5)=", float(model(torch.tensor([[0.5]]).float())[0][0]))


if __name__ == "__main__":
    ray.init()
    main()
