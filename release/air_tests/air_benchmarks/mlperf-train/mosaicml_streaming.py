import PIL
import ray

from streaming import LocalDataset, MDSWriter


class MosaicDataset(LocalDataset):
    def __init__(self,
                 local: str,
                 transforms: Callable
                ) -> None:
        super().__init__(local=local)
        self.transforms = transforms

    def __getitem__(self, idx:int) -> Any:
        obj = super().__getitem__(idx)
        image = obj['image']
        label = obj['label']
        return self.transforms(image), label

ds = ray.data.read_images("/home/ray/imagenet-1gb-data/dog")
it = ds.iter_rows()

columns = {"image": "jpeg", "label": "int"}
# If reading from local disk, should turn off compression and use
# streaming.LocalDataset.
# If uploading to S3, turn on compression (e.g., compression="snappy") and
# streaming.StreamingDataset.
with MDSWriter(out="/home/ray/mosaicml-data", columns=columns, compression=None) as out:
    for i, img in enumerate(it):
        out.write({
            "image": PIL.Image.fromarray(img["image"]),
            "label": 0,
        })
        if i % 10 == 0:
            print(f"Wrote {i} images.")


ds = MosaicDataset("/home/ray/mosaicml-data/", transforms=transform)
ds = torch.utils.data.DataLoader(ds, batch_size=32, num_workers=16)