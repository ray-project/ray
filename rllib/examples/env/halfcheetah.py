import numpy as np
#from gym.envs.mujoco import HalfCheetahEnv
import inspect


def get_all_function_arguments(function, locals):
    kwargs_dict = {}
    for arg in inspect.getfullargspec(function).kwonlyargs:
        if arg not in ["args", "kwargs"]:
            kwargs_dict[arg] = locals[arg]
    args = [locals[arg] for arg in inspect.getfullargspec(function).args]

    if "args" in locals:
        args += locals["args"]

    if "kwargs" in locals:
        kwargs_dict.update(locals["kwargs"])
    return args, kwargs_dict


import numpy as np
from gym import utils
from gym.envs.mujoco import mujoco_env

class HalfCheetahEnv(mujoco_env.MujocoEnv, utils.EzPickle):
    def __init__(self):
        mujoco_env.MujocoEnv.__init__(self, 'half_cheetah.xml', 5)
        utils.EzPickle.__init__(self)

    def step(self, action):
        xposbefore = self.sim.data.qpos[0]
        self.do_simulation(action, self.frame_skip)
        xposafter = self.sim.data.qpos[0]
        ob = self._get_obs()
        reward_ctrl = - 0.1 * np.square(action).sum()
        reward_run = ob[8]
        #reward_run = (xposafter - xposbefore)/self.dt
        reward = reward_ctrl + reward_run
        done = False
        return ob, reward, done, dict(reward_run=reward_run, reward_ctrl=reward_ctrl)

    def _get_obs(self):
        return np.concatenate([
            self.sim.data.qpos.flat[1:],
            self.sim.data.qvel.flat,
        ])

    def reset_model(self):
        qpos = self.init_qpos + self.np_random.uniform(low=-.1, high=.1, size=self.model.nq)
        qvel = self.init_qvel + self.np_random.randn(self.model.nv) * .1
        self.set_state(qpos, qvel)
        return self._get_obs()

    def viewer_setup(self):
        self.viewer.cam.distance = self.model.stat.extent * 0.5


class HalfCheetahWrapper(HalfCheetahEnv):
    def __init__(self, *args, **kwargs):
        HalfCheetahEnv.__init__(self, *args, **kwargs)

    def reward(self, obs, action, obs_next):
        if obs.ndim == 2 and action.ndim == 2:
            assert obs.shape == obs_next.shape
            forward_vel = obs_next[:, 8]
            ctrl_cost = 0.1 * np.sum(np.square(action), axis=1)
            reward = forward_vel - ctrl_cost
            return np.minimum(np.maximum(-1000.0, reward), 1000.0)
        else:
            forward_vel = obs_next[8]
            ctrl_cost = 0.1 * np.square(action).sum()
            reward = forward_vel - ctrl_cost
            return np.minimum(np.maximum(-1000.0, reward), 1000.0)


if __name__ == "__main__":
    env = HalfCheetahWrapper()
    env.reset()
    for _ in range(1000):
        env.step(env.action_space.sample())
