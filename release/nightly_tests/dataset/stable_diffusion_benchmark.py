import argparse
from typing import Dict

import numpy as np
import torch
from diffusers import StableDiffusionImg2ImgPipeline
from benchmark import Benchmark

import ray

DATA_URI = "s3://air-example-data-2/10G-image-data-synthetic-raw-parquet/"
# This isn't the largest batch size that fits in memory, but it achieves virtually 100%
# GPU utilization, and throughput declines at higher batch sizes.
BATCH_SIZE = 32
PROMPT = "ghibli style"


def parse_args():
    parser = argparse.ArgumentParser(description="Stable diffusion benchmark")
    parser.add_argument("--smoke-test", action="store_true")
    return parser.parse_args()


def main(args):
    ray.init()
    ray.data.DataContext.get_current().execution_options.verbose_progress = True

    dataset = ray.data.read_parquet(DATA_URI)

    if args.smoke_test:
        dataset = dataset.limit(1)

    actor_pool_size = int(ray.cluster_resources().get("GPU"))
    dataset = dataset.map_batches(
        GenerateImage,
        compute=ray.data.ActorPoolStrategy(size=actor_pool_size),
        batch_size=BATCH_SIZE,
        num_gpus=1,
    )
    dataset_iter = dataset.iter_batches(batch_format="pyarrow", batch_size=None)

    benchmark = Benchmark()
    benchmark.run_iterate_ds("main", dataset_iter)
    benchmark.write_result()


class GenerateImage:
    def __init__(self):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.pipeline = StableDiffusionImg2ImgPipeline.from_pretrained(
            "nitrosocke/Ghibli-Diffusion",
            torch_dtype=torch.float16,
            use_safetensors=True,
            requires_safety_checker=False,
            safety_checker=None,
        ).to(device)
        self.pipeline.set_progress_bar_config(disable=True)

    def __call__(self, batch: Dict[str, np.ndarray]):
        output = self.pipeline(
            prompt=[PROMPT] * len(batch["image"]),
            image=batch["image"],
            output_type="np",
        )
        return {"image": output.images}


if __name__ == "__main__":
    args = parse_args()
    main(args)
