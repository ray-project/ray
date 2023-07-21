import torch
import torchvision
import time

DEFAULT_IMAGE_SIZE = 224


def iterate(dataset, label, metrics):
    start = time.time()
    it = iter(dataset)
    num_rows = 0
    for batch in it:
        num_rows += len(batch)
    end = time.time()

    tput = num_rows / (end - start)
    metrics[label] = tput


def get_transform(to_torch_tensor, image_size=DEFAULT_IMAGE_SIZE):
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
        ]
        + [torchvision.transforms.ToTensor()]
        if to_torch_tensor
        else []
    )
    return transform


def crop_and_flip_image_batch(image_batch):
    transform = get_transform(False)
    batch_size, height, width, channels = image_batch["image"].shape
    tensor_shape = (batch_size, channels, height, width)
    image_batch["image"] = transform(
        torch.Tensor(image_batch["image"].reshape(tensor_shape))
    )
    return image_batch
