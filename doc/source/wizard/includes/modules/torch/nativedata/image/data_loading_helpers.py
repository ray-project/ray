import boto3
from pathlib import Path
import os

from torchvision import datasets, transforms
from torch.utils.data import DataLoader


def download_dataset_from_s3(destination_dir: str):
    destination_path = Path(destination_dir).resolve()
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket("air-example-data")
    for obj in bucket.objects.filter(Prefix="food-101-tiny"):
        os.makedirs(os.path.dirname(obj.key), exist_ok=True)
        download_path = str(destination_path / obj.key)
        if not os.path.exists(download_path):
            bucket.download_file(obj.key, download_path)


def get_datasets(global_batch_size: int, batch_size_per_worker: int) -> dict:
    download_dataset_from_s3(destination_dir=".")

    data_transforms = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
        ]
    )

    torch_datasets = {}
    for split in ["train", "valid"]:
        torch_datasets[split] = datasets.ImageFolder(
            os.path.join("./food-101-tiny", split), data_transforms
        )

    batch_size_per_worker = 64
    torch_datasets = build_datasets()

    dataloaders = {}
    for split, dataset in torch_datasets.items():
        dataloader = DataLoader(
            dataset=dataset, batch_size=batch_size_per_worker, shuffle=True
        )
        dataloaders[split] = ray.train.torch.prepare_data_loader(dataloader)

    return torch_datasets
