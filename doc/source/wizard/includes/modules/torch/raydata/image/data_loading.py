batch_size_per_worker = 32
IMG_SIZE = 224

datasets = {
    dataset_name: session.get_dataset_shard(dataset_name)
    for dataset_name in ("train", "valid")
}
datasets = {
    dataset_name: dataset.iter_torch_batches(
        batch_size=batch_size_per_worker, device=ray.train.torch.get_device()
    )
    for dataset_name, dataset in datasets.items()
}
