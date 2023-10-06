# __torch_train_base_start__
import tempfile
import torch
from torchvision.models import resnet18
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose
from torch.utils.data import DataLoader
from torch.optim import Adam
from torch.nn import CrossEntropyLoss
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, Checkpoint
import ray.train


def train_func(config):

    # Model, Loss, Optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    # [1] Prepare model.
    model = ray.train.torch.prepare_model(model)
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Data
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    train_data = FashionMNIST(
        root="./data", train=True, download=True, transform=transform
    )
    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    # [2] Prepare dataloader.
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(10):
        for images, labels in train_loader:
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        checkpoint_dir = tempfile.gettempdir()
        checkpoint_path = checkpoint_dir + "/model.checkpoint"
        torch.save(model.state_dict(), checkpoint_path)
        # [3] Report metrics and checkpoint.
        ray.train.report(
            {"loss": loss.item()}, checkpoint=Checkpoint.from_directory(checkpoint_dir)
        )


# [4] Configure scaling and resource requirements.
scaling_config = ScalingConfig(num_workers=2, use_gpu=True)

# [5] Launch distributed training job.
trainer = TorchTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
# __torch_train_base_end__
