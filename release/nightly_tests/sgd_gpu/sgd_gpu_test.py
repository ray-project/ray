import json
import os
import time

import ray
from ray.util.sgd.torch.examples.cifar_pytorch_example import train_cifar

if __name__ == "__main__":
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
    start_time = time.time()
    success = True
    try:
        train_cifar(num_workers=2, use_gpu=True, num_epochs=5, fp16=True,
                    test_mode=False)
    except Exception as e:
        print(f"The test failed with {e}")
        success = False

    delta = time.time() - start_time
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"shuffle_time": delta, "success": success}))