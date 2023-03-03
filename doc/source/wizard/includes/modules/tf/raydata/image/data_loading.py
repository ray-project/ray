batch_size_per_worker = 32
IMG_SIZE = 224

datasets = {
    dataset_name: session.get_dataset_shard(dataset_name)
    for dataset_name in ("train", "valid")
}
datasets = {
    dataset_name: dataset.to_tf(
        feature_columns=["image"],
        label_columns=["label"],
        batch_size=batch_size_per_worker,
        local_shuffle_buffer_size=256,
    ).map(
        lambda image, label: (
            tf.image.resize(image["image"], (IMG_SIZE, IMG_SIZE)),
            label["label"],
        )
    )
    for dataset_name, dataset in datasets.items()
}
