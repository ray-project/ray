"""
GRPO on GSM8k with vLLM generation, FSDP2, and Ray GPU objects.

Run: `python main.py --steps 10`.
"""

import atexit


import argparse
from itertools import cycle
from typing import Any
from generator import Generator
from learner import Learner
from replay_buffer import ReplayBuffer
from settings import (
    DATASET_SPLIT,
    DATASET_SAMPLES,
    BATCH_SIZE,
    LOG_EVERY,
    RAY_NAMESPACE,
    STEPS,
    REGISTRY_NAME,
    WORLD_SIZE_PER_MODEL,
)


import ray

from data_util import load_gsm8k_dataset, Scorer


def _batch_iter(items: list[tuple[str, str]], batch_size: int):
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


@ray.remote(name=REGISTRY_NAME)
class RayObjectRefRegistry:
    """Store ObjectRefs in a global actor without materializing them.

    Wrap refs in a container to avoid eager materialization until they reach the vLLM workers.
    """

    def __init__(self):
        self.storage = []

    def reset(self):
        self.storage = []

    def put(self, ref_in_container: list[ray.ObjectRef]):
        self.storage.append(ref_in_container)

    def get(self) -> list[ray.ObjectRef]:
        if not len(self.storage):
            raise RuntimeError("RayObjectRefRegistry is empty. put() weights first.")
        # Unbundle each contained ref from its list container.
        weight_refs = [contained_ref[0] for contained_ref in self.storage]
        return weight_refs


def train(
    total_steps: int,
) -> dict[str, Any]:
    """Run one end-to-end training session."""

    # Create one instance of each actor.
    replay_buf = ReplayBuffer.remote()
    _ref_registry = RayObjectRefRegistry.remote()
    atexit.register(lambda: ray.kill(_ref_registry))
    learner = Learner(replay_buf)
    scorer = Scorer.remote(replay_buf)
    generator = Generator(scorer)

    # Initialize the generator with current learner weights.
    learner.get_weights()
    generator.update_weights()

    # Load a small GSM8k sample for the demo.
    items = load_gsm8k_dataset(split=DATASET_SPLIT, sample_count=DATASET_SAMPLES)
    batches = list(_batch_iter(items, BATCH_SIZE))
    batch_cycle = cycle(batches)

    # Pre-fill the replay buffer before starting GRPO.
    generator.generate(next(batch_cycle))

    step_results = []
    losses = []
    for i in range(total_steps):
        print(f"[train] Starting step {i + 1}/{total_steps}", flush=True)
        prompts_batch = next(batch_cycle)
        generator.generate(prompts_batch)

        # Asynchronously log every LOG_EVERY steps.
        # Each step produces WORLD_SIZE_PER_MODEL results.
        if (num_results := len(step_results)) >= LOG_EVERY * WORLD_SIZE_PER_MODEL:
            for step_result in ray.get(step_results):
                losses.append(step_result["loss"])
            print(
                f"Step {i + 1}/{total_steps} | Loss: {sum(losses[-num_results:]) / num_results}"
            )
            step_results.clear()

        step_results.extend(learner.step())

        # Update the generator with new weights.
        learner.get_weights()
        generator.update_weights()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GRPO with Ray GPU objects, vLLM, FSDP"
    )
    parser.add_argument("--steps", type=int, default=STEPS)
    args = parser.parse_args()

    ray.init(
        ignore_reinit_error=True,
        namespace=RAY_NAMESPACE,
    )
    train(total_steps=args.steps)
    print("Finished!")
