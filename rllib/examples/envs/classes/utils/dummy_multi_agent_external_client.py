import time

from ray.rllib.env.external.multi_agent_rllib_gateway import \
    MultiAgentRLlibGateway
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


def policy_mapping_fn(agent_id, *_, **__):
    return f"p{agent_id}"


def _dummy_multi_agent_external_client(client_id: int, port: int = 5556):
    """
    A dummy client that runs MultiAgentCartPole and acts as a testing external env.
    """
    gateway = MultiAgentRLlibGateway(port=port)

    # wait for gateway to connect to server
    while not gateway.is_initialized:
        time.sleep(5)

    print(f"Setup completed for client {client_id} with server on port {port}")

    config = gateway._config
    env = MultiAgentCartPole(config={"num_agents": len(config.policies)})

    while True:
        obs, _ = env.reset()
        prev_reward, terminated, truncated = {}, {}, {}
        while True:
            action = gateway.get_action(
                prev_observation=obs,
                prev_reward=prev_reward,
                terminated=terminated,
                truncated=truncated,
            )
            obs, prev_reward, terminated, truncated, _ = env.step(action)

            if terminated.get("__all__", False) or truncated.get("__all__", False):
                # make sure to add last timestep to episode before resetting
                _ = gateway.get_action(
                    prev_observation=obs,
                    prev_reward=prev_reward,
                    terminated=terminated,
                    truncated=truncated,
                )
                break
