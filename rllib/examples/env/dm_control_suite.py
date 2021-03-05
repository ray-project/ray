from ray.rllib.env.wrappers.dm_control_wrapper import DMCEnv
"""
8 Environments from Deepmind Control Suite
"""


def acrobot_swingup(from_pixels=True,
                    height=64,
                    width=64,
                    frame_skip=2,
                    channels_first=True):
    return DMCEnv(
        "acrobot",
        "swingup",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def walker_walk(from_pixels=True,
                height=64,
                width=64,
                frame_skip=2,
                channels_first=True):
    return DMCEnv(
        "walker",
        "walk",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def hopper_hop(from_pixels=True,
               height=64,
               width=64,
               frame_skip=2,
               channels_first=True):
    return DMCEnv(
        "hopper",
        "hop",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def hopper_stand(from_pixels=True,
                 height=64,
                 width=64,
                 frame_skip=2,
                 channels_first=True):
    return DMCEnv(
        "hopper",
        "stand",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def cheetah_run(from_pixels=True,
                height=64,
                width=64,
                frame_skip=2,
                channels_first=True):
    return DMCEnv(
        "cheetah",
        "run",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def walker_run(from_pixels=True,
               height=64,
               width=64,
               frame_skip=2,
               channels_first=True):
    return DMCEnv(
        "walker",
        "run",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def pendulum_swingup(from_pixels=True,
                     height=64,
                     width=64,
                     frame_skip=2,
                     channels_first=True):
    return DMCEnv(
        "pendulum",
        "swingup",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def cartpole_swingup(from_pixels=True,
                     height=64,
                     width=64,
                     frame_skip=2,
                     channels_first=True):
    return DMCEnv(
        "cartpole",
        "swingup",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)


def humanoid_walk(from_pixels=True,
                  height=64,
                  width=64,
                  frame_skip=2,
                  channels_first=True):
    return DMCEnv(
        "humanoid",
        "walk",
        from_pixels=from_pixels,
        height=height,
        width=width,
        frame_skip=frame_skip,
        channels_first=channels_first)
