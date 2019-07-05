import numpy as np
from gym import utils
from gym.envs.mujoco import mujoco_env

class ThrowerEnv(mujoco_env.MujocoEnv, utils.EzPickle):
    def __init__(self):
        utils.EzPickle.__init__(self)
        self._ball_hit_ground = False
        self._ball_hit_location = None
        mujoco_env.MujocoEnv.__init__(self, 'thrower.xml', 5)

    def step(self, a):
        ball_xy = self.get_body_com("ball")[:2]
        goal_xy = self.get_body_com("goal")[:2]

        if not self._ball_hit_ground and self.get_body_com("ball")[2] < -0.25:
            self._ball_hit_ground = True
            self._ball_hit_location = self.get_body_com("ball")

        if self._ball_hit_ground:
            ball_hit_xy = self._ball_hit_location[:2]
            reward_dist = -np.linalg.norm(ball_hit_xy - goal_xy)
        else:
            reward_dist = -np.linalg.norm(ball_xy - goal_xy)
        reward_ctrl = - np.square(a).sum()

        reward = reward_dist + 0.002 * reward_ctrl
        self.do_simulation(a, self.frame_skip)
        ob = self._get_obs()
        done = False
        return ob, reward, done, dict(reward_dist=reward_dist,
                reward_ctrl=reward_ctrl)

    def viewer_setup(self):
        self.viewer.cam.trackbodyid = 0
        self.viewer.cam.distance = 4.0

    def reset_model(self):
        self._ball_hit_ground = False
        self._ball_hit_location = None

        qpos = self.init_qpos
        self.goal = np.array([self.np_random.uniform(low=-0.3, high=0.3),
                              self.np_random.uniform(low=-0.3, high=0.3)])

        qpos[-9:-7] = self.goal
        qvel = self.init_qvel + self.np_random.uniform(low=-0.005,
                high=0.005, size=self.model.nv)
        qvel[7:] = 0
        self.set_state(qpos, qvel)
        return self._get_obs()

    def _get_obs(self):
        return np.concatenate([
            self.sim.data.qpos.flat[:7],
            self.sim.data.qvel.flat[:7],
            self.get_body_com("r_wrist_roll_link"),
            self.get_body_com("ball"),
            self.get_body_com("goal"),
        ])
