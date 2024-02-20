import gymnasium as gym
import numpy as np
from PIL import Image, ImageDraw

from gymnasium.envs.classic_control.cartpole import CartPoleEnv

from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class CartPoleDebug(CartPoleEnv):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        low = np.concatenate([np.array([0.0]), self.observation_space.low])
        high = np.concatenate([np.array([1000.0]), self.observation_space.high])

        self.observation_space = gym.spaces.Box(low, high, shape=(5,), dtype=np.float32)

        self.timesteps_ = 0
        self._next_action = 0
        self._seed = 1

    def reset(self, *, seed=None, options=None):
        ret = super().reset(seed=self._seed)
        self._seed += 1
        self.timesteps_ = 0
        self._next_action = 0
        obs = np.concatenate([np.array([self.timesteps_]), ret[0]])
        return obs, ret[1]

    def step(self, action):
        ret = super().step(self._next_action)

        self.timesteps_ += 1
        self._next_action = 0 if self._next_action else 1

        obs = np.concatenate([np.array([self.timesteps_]), ret[0]])
        reward = 0.1 * self.timesteps_
        return (obs, reward) + ret[2:]


gym.register("CartPoleDebug-v0", CartPoleDebug)
cartpole_env = gym.make("CartPoleDebug-v0", render_mode="rgb_array")
cartpole_env.reset()

frozenlake_env = gym.make(
    "FrozenLake-v1", render_mode="rgb_array", is_slippery=False, map_name="4x4"
)  # desc=["SF", "HG"])
frozenlake_env.reset()


def create_cartpole_dream_image(
    dreamed_obs,  # real space (not symlog'd)
    dreamed_V,  # real space (not symlog'd)
    dreamed_a,
    dreamed_r_tp1,  # real space (not symlog'd)
    dreamed_ri_tp1,  # intrinsic reward
    dreamed_c_tp1,  # continue flag
    value_target,  # real space (not symlog'd)
    initial_h,
    as_tensor=False,
):
    # CartPoleDebug
    if dreamed_obs.shape == (5,):
        # Set the state of our env to the given observation.
        cartpole_env.unwrapped.state = np.array(dreamed_obs[1:], dtype=np.float32)
    # Normal CartPole-v1
    else:
        cartpole_env.unwrapped.state = np.array(dreamed_obs, dtype=np.float32)

    # Produce an RGB-image of the current state.
    rgb_array = cartpole_env.render()

    # Add value-, action-, reward-, and continue-prediction information.
    image = Image.fromarray(rgb_array)
    draw_obj = ImageDraw.Draw(image)

    # fnt = ImageFont.load_default(size=40)

    draw_obj.text(
        (5, 6), f"Vt={dreamed_V:.2f} (Rt={value_target:.2f})", fill=(0, 0, 0)
    )  # , font=fnt.font, size=30)
    draw_obj.text(
        (5, 18),
        f"at={'<--' if dreamed_a == 0 else '-->'} ({dreamed_a})",
        fill=(0, 0, 0),
    )
    draw_obj.text((5, 30), f"rt+1={dreamed_r_tp1:.2f}", fill=(0, 0, 0))
    if dreamed_ri_tp1 is not None:
        draw_obj.text((5, 42), f"rit+1={dreamed_ri_tp1:.6f}", fill=(0, 0, 0))
    draw_obj.text((5, 54), f"ct+1={dreamed_c_tp1}", fill=(0, 0, 0))
    draw_obj.text((5, 66), f"|h|t={np.mean(np.abs(initial_h)):.5f}", fill=(0, 0, 0))

    if dreamed_obs.shape == (5,):
        draw_obj.text((20, 100), f"t={dreamed_obs[0]}", fill=(0, 0, 0))

    # Return image.
    np_img = np.asarray(image)
    if as_tensor:
        return tf.convert_to_tensor(np_img, dtype=tf.uint8)
    return np_img


def create_frozenlake_dream_image(
    dreamed_obs,  # real space (not symlog'd)
    dreamed_V,  # real space (not symlog'd)
    dreamed_a,
    dreamed_r_tp1,  # real space (not symlog'd)
    dreamed_ri_tp1,  # intrinsic reward
    dreamed_c_tp1,  # continue flag
    value_target,  # real space (not symlog'd)
    initial_h,
    as_tensor=False,
):
    frozenlake_env.unwrapped.s = np.argmax(dreamed_obs, axis=0)

    # Produce an RGB-image of the current state.
    rgb_array = frozenlake_env.render()

    # Add value-, action-, reward-, and continue-prediction information.
    image = Image.fromarray(rgb_array)
    draw_obj = ImageDraw.Draw(image)

    draw_obj.text((5, 6), f"Vt={dreamed_V:.2f} (Rt={value_target:.2f})", fill=(0, 0, 0))
    action_arrow = (
        "<--"
        if dreamed_a == 0
        else "v"
        if dreamed_a == 1
        else "-->"
        if dreamed_a == 2
        else "^"
    )
    draw_obj.text((5, 18), f"at={action_arrow} ({dreamed_a})", fill=(0, 0, 0))
    draw_obj.text((5, 30), f"rt+1={dreamed_r_tp1:.2f}", fill=(0, 0, 0))
    if dreamed_ri_tp1 is not None:
        draw_obj.text((5, 42), f"rit+1={dreamed_ri_tp1:.6f}", fill=(0, 0, 0))
    draw_obj.text((5, 54), f"ct+1={dreamed_c_tp1}", fill=(0, 0, 0))
    draw_obj.text((5, 66), f"|h|t={np.mean(np.abs(initial_h)):.5f}", fill=(0, 0, 0))

    # Return image.
    np_img = np.asarray(image)
    if as_tensor:
        return tf.convert_to_tensor(np_img, dtype=tf.uint8)
    return np_img


if __name__ == "__main__":
    # CartPole debug.
    rgb_array = create_cartpole_dream_image(
        dreamed_obs=np.array([100.0, 1.0, -0.01, 1.5, 0.02]),
        dreamed_V=4.3,
        dreamed_a=1,
        dreamed_r_tp1=1.0,
        dreamed_c_tp1=True,
        initial_h=0.0,
        value_target=8.0,
    )
    # ImageFont.load("arial.pil")
    image = Image.fromarray(rgb_array)
    image.show()

    # Normal CartPole.
    rgb_array = create_cartpole_dream_image(
        dreamed_obs=np.array([1.0, -0.01, 1.5, 0.02]),
        dreamed_V=4.3,
        dreamed_a=1,
        dreamed_r_tp1=1.0,
        dreamed_c_tp1=True,
        initial_h=0.1,
        value_target=8.0,
    )
    # ImageFont.load("arial.pil")
    image = Image.fromarray(rgb_array)
    image.show()

    # Frozenlake
    rgb_array = create_frozenlake_dream_image(
        dreamed_obs=np.array([1.0] + [0.0] * (frozenlake_env.observation_space.n - 1)),
        dreamed_V=4.3,
        dreamed_a=1,
        dreamed_r_tp1=1.0,
        dreamed_c_tp1=True,
        initial_h=0.1,
        value_target=8.0,
    )
    image = Image.fromarray(rgb_array)
    image.show()
