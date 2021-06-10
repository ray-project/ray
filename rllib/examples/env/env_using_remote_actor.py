"""
Example of an environment that uses a named remote actor as parameter
server.

"""
from gym.envs.classic_control.cartpole import CartPoleEnv
from gym.utils import seeding

import ray


@ray.remote
class ParameterStorage:
    def get_params(self, rng):
        return {
            "MASSCART": rng.uniform(low=0.5, high=2.0),
        }


class CartPoleWithRemoteParamServer(CartPoleEnv):
    """CartPoleMassEnv varies the weights of the cart and the pole.
    """

    def __init__(self, env_config):
        self.env_config = env_config
        super().__init__()
        # Get our param server (remote actor) by name.
        self._handler = ray.get_actor(
            env_config.get("param_server", "param-server"))

    # def seed(self, seed=None):
    #     print(f"Seeding env (worker={self.env_config.worker_index}) "
    #           f"with {seed}")
    #     self.np_random, seed = seeding.np_random(seed)
    #     return [seed]

    def reset(self):
        # Pass in our RNG to guarantee no race conditions.
        # If `self._handler` had its own RNG, this may clash with other
        # envs trying to use the same param-server.
        params = ray.get(self._handler.get_params.remote(self.np_random))

        # IMPORTANT: Advance the state of our RNG (self._rng was passed
        # above via ray (serialized) and thus not altered locally here!).
        # Or create a new RNG from another random number:
        new_seed = self.np_random.randint(0, 1000000)
        self.np_random, _ = seeding.np_random(new_seed)

        print(f"Env worker-idx={self.env_config.worker_index} "
              f"mass={params['MASSCART']}")

        self.masscart = params["MASSCART"]
        self.total_mass = (self.masspole + self.masscart)
        self.polemass_length = (self.masspole * self.length)

        return super().reset()
