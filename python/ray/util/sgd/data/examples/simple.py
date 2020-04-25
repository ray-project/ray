import ray
from ray.util import iter
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


def to_mat(x):
    return torch.tensor([[x]]).float()


def model_creator(config):
    return Net()


data = [i * 0.001 for i in range(1000)]


def data_creator(config):
    as_list = list(map(lambda x: (to_mat(x), to_mat(x)), data))
    return torch.utils.data.DataLoader(as_list)


def optimizer_creator(model, config):
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def loss_creator(config):
    return -nn.MSELoss(config)


ray.init()

p_iter = iter.from_items(data, num_shards=1, repeat=True)
dataset = Dataset(
    p_iter,
    batch_size=32,
    max_concur=1,
    download_func=lambda x: (to_mat(x), to_mat(x)))

trainer = TorchTrainer(
    model_creator=model_creator,
    data_creator=data_creator,
    optimizer_creator=optimizer_creator,
    loss_creator=torch.nn.MSELoss,
    config={
        "batch_size": 32,
        "epoch": 10
    },
    num_workers=5,
)

for i in range(10):
    # Train a full epoch using the data_creator
    trainer.train()
    # Train for another epoch using the dataset
    trainer.train(dataset=dataset, num_steps=200)

    model = trainer.get_model()
    print("f(0.5)=", float(model(to_mat(0.5))[0][0]))
