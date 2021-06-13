import pathlib


def get_default_workflow_root_dir():
    return pathlib.Path.cwd() / ".rayflow"
