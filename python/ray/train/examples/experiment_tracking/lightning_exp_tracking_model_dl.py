import torch
import torch.nn.functional as F
import pytorch_lightning as pl
from torch.utils.data import TensorDataset, DataLoader

# create dummy data
X = torch.randn(128, 3)  # 128 samples, 3 features
y = torch.randint(0, 2, (128,))  # 128 binary labels

# create a TensorDataset to wrap the data
dataset = TensorDataset(X, y)

# create a DataLoader to iterate over the dataset
batch_size = 8
dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

# Define a dummy model
class DummyModel(pl.LightningModule):
    def __init__(self):
        super().__init__()
        self.layer = torch.nn.Linear(3, 1)

    def forward(self, x):
        return self.layer(x)

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.binary_cross_entropy_with_logits(y_hat.flatten(), y.float())

        # The metrics below will be reported to Loggers
        self.log("train_loss", loss)
        self.log_dict({"metric_1": 1 / (batch_idx + 1), "metric_2": batch_idx * 100})
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=1e-3)
    