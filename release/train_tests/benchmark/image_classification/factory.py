import torch
import ray.train

from dataloader_factory import BaseDataLoaderFactory


def mock_dataloader(num_batches: int = 64, batch_size: int = 32):
    device = ray.train.torch.get_device()

    images = torch.randn(batch_size, 3, 224, 224).to(device)
    labels = torch.randint(0, 1000, (batch_size,)).to(device)

    for _ in range(num_batches):
        yield images, labels


class ImageClassificationMockDataLoaderFactory(BaseDataLoaderFactory):
    def get_train_dataloader(self):
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=1024, batch_size=dataloader_config.train_batch_size
        )

    def get_val_dataloader(self):
        dataloader_config = self.get_dataloader_config()
        return mock_dataloader(
            num_batches=512, batch_size=dataloader_config.validation_batch_size
        )
