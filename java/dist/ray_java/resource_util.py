import os


def get_ray_jars_dir():
    """Return a directory where all ray-related jars and
      their dependencies locate."""
    current_dir = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(current_dir, "jars"))
