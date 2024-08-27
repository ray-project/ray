import json
from typing import Dict

from starlette.requests import Request

from ray import serve
from ray.serve.schema import LoggingConfig
from ray.rllib.algorithms.algorithm import Algorithm


@serve.deployment(
    route_prefix="/rllib-rlmodule",
    logging_config=LoggingConfig(log_level="WARN"),
)
class ServeRLlibRLModule:
    """Callable class used by Ray Serve to handle async requests.

    All the necessary serving logic is implemented in here:
    - Creation and restoring of the (already trained) RLlib Algorithm.
    - Calls to algo.compute_action upon receiving an action request
      (with a current observation).
    """

    def __init__(self, checkpoint):
        self.algo = Algorithm.from_checkpoint(checkpoint)

    async def __call__(self, starlette_request: Request) -> Dict:
        request = await starlette_request.body()
        request = request.decode("utf-8")
        request = json.loads(request)
        obs = request["observation"]

        # Compute and return the action for the given observation.
        action = self.algo.compute_single_action(obs)

        return {"action": int(action)}


# Defining the builder function. This is so we can start our deployment via:
# `serve run [this py module]:rl_module checkpoint=[some algo checkpoint path]`
def rl_module(args: Dict[str, str]):
    return ServeRLlibRLModule.bind(args["checkpoint"])
