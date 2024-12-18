import json
from typing import Dict

import numpy as np
from starlette.requests import Request
import torch

from ray import serve
from ray.rllib.core import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.serve.schema import LoggingConfig


@serve.deployment(logging_config=LoggingConfig(log_level="WARN"))
class ServeRLlibRLModule:
    """Callable class used by Ray Serve to handle async requests.

    All the necessary serving logic is implemented in here:
    - Creation and restoring of the (already trained) RLlib Algorithm.
    - Calls to algo.compute_action upon receiving an action request
      (with a current observation).
    """

    def __init__(self, rl_module_checkpoint):
        self.rl_module = RLModule.from_checkpoint(rl_module_checkpoint)

    async def __call__(self, starlette_request: Request) -> Dict:
        request = await starlette_request.body()
        request = request.decode("utf-8")
        request = json.loads(request)
        obs = request["observation"]

        # Compute and return the action for the given observation (create a batch
        # with B=1 and convert to torch).
        output = self.rl_module.forward_inference(
            batch={"obs": torch.from_numpy(np.array([obs], np.float32))}
        )
        # Extract action logits and unbatch.
        logits = output[Columns.ACTION_DIST_INPUTS][0]
        # Act greedily (argmax).
        action = int(np.argmax(logits))

        return {"action": action}


# Defining the builder function. This is so we can start our deployment via:
# `serve run [this py module]:rl_module checkpoint=[some algo checkpoint path]`
def rl_module(args: Dict[str, str]):
    serve.start(http_options={"host": "0.0.0.0", "port": args.get("port", 12345)})
    return ServeRLlibRLModule.bind(args["rl_module_checkpoint"])
