import hashlib
from os import path

import time
import torch
import ray

from flags import run_model_flags
from generate_utils import get_pipeline


def run(args):
    class StableDiffusionCallable:
        def __init__(self, model_dir, output_dir, lora_weights_dir=None):
            print(f"Loading model from {model_dir}")
            self.pipeline = get_pipeline(model_dir, lora_weights_dir)
            self.pipeline.set_progress_bar_config(disable=True)
            if torch.cuda.is_available():
                self.pipeline.to("cuda")
            self.output_dir = output_dir

        def __call__(self, batch):
            filenames = []
            for i, prompt in zip(batch["idx"], batch["prompt"]):
                # Generate 1 image at a time to reduce memory consumption.
                for image in self.pipeline(prompt).images:
                    hash_image = hashlib.sha1(image.tobytes()).hexdigest()
                    image_filename = path.join(self.output_dir, f"{i}-{hash_image}.jpg")
                    image.save(image_filename)
                    print(f"Saved {image_filename}")
                    filenames.append(image_filename)
            return {"filename": filenames}

    prompts = args.prompts.split(",")

    start_time = time.time()
    num_samples = len(prompts) * args.num_samples_per_prompt

    if args.use_ray_data:
        # Use Ray Data to perform batch inference to generate many images in parallel
        prompts_with_idxs = []
        for prompt in prompts:
            prompts_with_idxs.extend(
                [
                    {"idx": i, "prompt": prompt}
                    for i in range(args.num_samples_per_prompt)
                ]
            )

        prompt_ds = ray.data.from_items(prompts_with_idxs)
        num_workers = 4

        # Run the batch inference by consuming output with `take_all`.
        prompt_ds.map_batches(
            StableDiffusionCallable,
            compute=ray.data.ActorPoolStrategy(size=num_workers),
            fn_constructor_args=(args.model_dir, args.output_dir),
            num_gpus=1,
            batch_size=num_samples // num_workers,
        ).take_all()

    else:
        # Generate images one by one
        stable_diffusion_predictor = StableDiffusionCallable(
            args.model_dir, args.output_dir, args.lora_weights_dir
        )
        for prompt in prompts:
            for i in range(args.num_samples_per_prompt):
                stable_diffusion_predictor({"idx": [i], "prompt": [prompt]})

    elapsed = time.time() - start_time
    print(
        f"Generated and saved {num_samples} images to {args.output_dir} in "
        f"{elapsed} seconds."
    )


if __name__ == "__main__":
    args = run_model_flags().parse_args()
    run(args)
