import os


def get_ray_jars_dir():
    """Return a jar directory where all ray, ray streaming and all their dependencies
    jars locate."""
    current_dir = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(current_dir, "jars"))
