import argparse
import time

import numpy as np
import pandas as pd

import ray
from ray.air.config import DatasetConfig
from ray.air.util.check_ingest import DummyTrainer
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.data.datasource import ImageFolderDatasource
from ray.data.preprocessors import BatchMapper
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor, to_air_checkpoint

GiB = 1024 * 1024 * 1024


def dummy_predict(data: "pd.DataFrame") -> "pd.DataFrame":
    # For 20k records (200GiB), this amounts to 2000 seconds of work.
    time.sleep(len(data) * 0.0001)
    return pd.DataFrame({"label": [42] * len(data)})


def make_ds(size_gb: int):
    # Dataset of 10KiB tensor records.
    total_size = 1024 * 1024 * 1024 * size_gb
    record_dim = 1280
    record_size = record_dim * 8
    num_records = int(total_size / record_size)
    dataset = ray.data.range_tensor(num_records, shape=(record_dim,))
    print("Created dataset", dataset, "of size", dataset.size_bytes())
    return dataset


def run_ingest_bulk(dataset, num_workers):
    dummy_prep = BatchMapper(lambda df: df * 2)
    trainer = DummyTrainer(
        scaling_config={
            "num_workers": num_workers,
            "trainer_resources": {"CPU": 0},
            "resources_per_worker": {"CPU": 3},
        },
        datasets={"train": dataset},
        preprocessor=dummy_prep,
        num_epochs=1,
        prefetch_blocks=1,
        dataset_config={"train": DatasetConfig(split=True)},
    )
    trainer.fit()


def run_ingest_streaming(dataset, num_workers):
    dummy_prep = BatchMapper(lambda df: df * 2)
    trainer = DummyTrainer(
        scaling_config={
            "num_workers": num_workers,
            "trainer_resources": {"CPU": 0},
            "resources_per_worker": {"CPU": 3},
        },
        datasets={"train": dataset},
        preprocessor=dummy_prep,
        num_epochs=1,
        prefetch_blocks=1,
        dataset_config={
            "train": DatasetConfig(
                split=True, use_stream_api=True, stream_window_size=num_workers * GiB
            )
        },
    )
    trainer.fit()


def run_infer_bulk(
    dataset,
    num_workers,
    post=None,
    stream=False,
    window_size_gb=10,
    images=False,
    use_gpu=False,
):
    start = time.time()

    if images:
        from PIL import Image
        from torchvision import transforms
        from torchvision.models import resnet18

        def preprocess(df):
            preprocess = transforms.Compose(
                [
                    transforms.Resize(256),
                    transforms.CenterCrop(224),
                    transforms.ToTensor(),
                    transforms.Normalize(
                        mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                    ),
                ]
            )
            if "image" not in df:
                # Gen a synthetic image.
                images = df["__value__"].map(
                    lambda i: preprocess(
                        Image.fromarray(np.random.rand(1024, 768, 3).astype(np.uint8))
                    )
                )
                images = [np.array(i) for i in images]
                df["image"] = TensorArray(images)
                del df["__value__"]
            else:
                df["image"] = TensorArray(
                    [preprocess(image.to_numpy()) for image in df["image"]]
                )
            return df

        preprocessor = BatchMapper(preprocess)

        model = resnet18(pretrained=True)

        ckpt = to_air_checkpoint(model=model, preprocessor=preprocessor)

        predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
    else:

        def fn(batch):
            return batch * 2

        dummy_prep = BatchMapper(fn)
        predictor = BatchPredictor.from_pandas_udf(dummy_predict)
        predictor.set_preprocessor(dummy_prep)

    if stream:
        result = predictor.predict_pipelined(
            dataset,
            bytes_per_window=int(window_size_gb * 1024 * 1024 * 1024),
            batch_size=1024 // 8,
            min_scoring_workers=num_workers,
            max_scoring_workers=num_workers,
            num_cpus_per_worker=1,
            num_gpus_per_worker=1 if use_gpu else 0,
            feature_columns=["image"] if use_gpu else None,
        )
    else:
        result = predictor.predict(
            dataset,
            batch_size=1024 // 8,
            min_scoring_workers=num_workers,
            max_scoring_workers=num_workers,
            num_cpus_per_worker=1,
            num_gpus_per_worker=1 if use_gpu else 0,
            feature_columns=["image"] if use_gpu else None,
        )
    if post:
        post(result)
    print(result.stats())
    print("Total runtime", time.time() - start)


def run_infer_streaming(dataset, num_workers, window_size_gb, images, use_gpu):
    def post(result):
        for b in result.iter_batches():
            pass

    return run_infer_bulk(
        dataset,
        num_workers,
        post,
        stream=True,
        window_size_gb=window_size_gb,
        images=images,
        use_gpu=use_gpu,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark", type=str, default="ingest", help="ingest/infer/infer_images"
    )
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--dataset-size-gb", type=float, default=200)
    parser.add_argument("--streaming", action="store_true", default=False)
    parser.add_argument("--use-gpu", action="store_true", default=False)
    parser.add_argument("--window-size-gb", type=float, default=10)
    parser.add_argument("--s3-data", type=str, default="")
    parser.add_argument("--s3-images", type=str, default="")
    args = parser.parse_args()
    if args.s3_data:
        ds = ray.data.read_parquet(args.s3_data)
    elif args.s3_images:
        assert args.benchmark == "infer_images"
        ds = ray.data.read_datasource(ImageFolderDatasource(), paths=[args.s3_images])
    else:
        ds = make_ds(args.dataset_size_gb)
    if args.benchmark == "ingest":
        if args.streaming:
            run_ingest_streaming(ds, args.num_workers)
        else:
            run_ingest_bulk(ds, args.num_workers)
    elif args.benchmark in ["infer", "infer_images"]:
        if args.streaming:
            run_infer_streaming(
                ds,
                args.num_workers,
                args.window_size_gb,
                images=args.benchmark == "infer_images",
                use_gpu=args.use_gpu,
            )
        else:
            run_infer_bulk(
                ds,
                args.num_workers,
                images=args.benchmark == "infer_images",
                use_gpu=args.use_gpu,
            )
    else:
        assert False
