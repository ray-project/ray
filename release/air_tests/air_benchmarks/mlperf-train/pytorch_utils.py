import torch
import torchvision
import os


DEFAULT_IMAGE_SIZE = 224


def build_torch_dataset(root_dir, batch_size, shuffle=False, num_workers=None):
    if num_workers is None:
        num_workers = os.cpu_count()
    # Note(swang): This is a different order from tf.data.
    # torch: decode -> randCrop+resize -> randFlip
    # tf.data: decode -> randCrop -> randFlip -> resize
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
            torchvision.transforms.ToTensor(),
        ]
    )

    data = torchvision.datasets.ImageFolder(root_dir, transform=transform)
    data_loader = torch.utils.data.DataLoader(
        data, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers
    )
    return data_loader
