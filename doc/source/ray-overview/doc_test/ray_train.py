import torch

import ray.train as train
from ray.train import Trainer


def train_func():
    # Setup model.
    model = torch.nn.Linear(1, 1)
    model = train.torch.prepare_model(model)
    loss_fn = torch.nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-2)

    # Setup data.
    input = torch.randn(1000, 1)
    labels = input * 2
    dataset = torch.utils.data.TensorDataset(input, labels)
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=32)
    dataloader = train.torch.prepare_data_loader(dataloader)

    # Train.
    for _ in range(5):
        for X, y in dataloader:
            pred = model(X)
            loss = loss_fn(pred, y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

    return model.state_dict()


trainer = Trainer(backend="torch", num_workers=4)
trainer.start()
results = trainer.run(train_func)
trainer.shutdown()

print(results)
