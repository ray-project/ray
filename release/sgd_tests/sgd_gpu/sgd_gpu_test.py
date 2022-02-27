import json
import os
import time

import ray
from ray.util.sgd.torch.examples.cifar_pytorch_example import train_cifar
import traceback

if __name__ == "__main__":
    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"))
    start_time = time.time()
    success = True
    try:
        from apex import amp  # noqa: F401
    except ImportError:
        traceback.print_exc()
        success = False

    try:
        train_cifar(
            num_workers=1, use_gpu=True, num_epochs=5, fp16=True, test_mode=False
        )
    except Exception as e:
        print(f"(native fp16) The test failed with {e}")
        success = False

    try:
        train_cifar(
            num_workers=1, use_gpu=True, num_epochs=5, fp16="apex", test_mode=False
        )
    except Exception as e:
        print(f"(apex fp16) The test failed with {e}")
        success = False

    delta = time.time() - start_time
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"train_time": delta, "success": success}))
