import ray
from ray.util.sgd.torch.torch_trainer import TorchTrainer
from ray.util.sgd.data.dataset import Dataset

import torch
from torch import nn
import torch.nn.functional as F


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


def model_creator(config):
    return Net()


def optimizer_creator(model, config):
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def to_mat(x):
    return torch.tensor([[x]]).float()


def dataset_creator():
    num_points = 32 * 100 * 2
    data = [i * (1 / num_points) for i in range(num_points)]
    dataset = Dataset(
        data,
        batch_size=32,
        max_concurrency=2,
        download_func=lambda x: (to_mat(x), to_mat(x)))
    return dataset


def main():
    dataset = dataset_creator()
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=None,
        optimizer_creator=optimizer_creator,
        loss_creator=torch.nn.MSELoss,
        num_workers=2,
    )

    for i in range(10):
        # Train a full epoch using the data_creator
        # trainer.train()

        # Train for another epoch using the dataset
        trainer.train(dataset=dataset, num_steps=100)

        model = trainer.get_model()
        print("f(0.5)=", float(model(to_mat(0.5))[0][0]))


if __name__ == "__main__":
    ray.init()
    main()
