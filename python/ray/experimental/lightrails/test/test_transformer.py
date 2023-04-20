#!/usr/bin/env python3

from ray.experimental.lightrails.model.gpt import Inferencer

if __name__ == "__main__":
    import ray

    inferencer = Inferencer(requires_gpu=False, batch_size=2)

    ids = [inferencer.generate("hello darkness my old") for _ in range(2)]

    for _ in range(100):
        inferencer._send_batches()
        inferencer._get_results()
