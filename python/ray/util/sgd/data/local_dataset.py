import os

from ray.util.sgd.data.dataset import Dataset


class LocalDataset(Dataset):
    """
    LocalDataset is a datatset which only exists on the disk of the driver node
    """

    def __init__(self, loc, max_paths=None, transform=None):
        super(LocalDataset, self).__init__(
            loc, remote=False, max_paths=max_paths, transform=transform)

    def get_paths(self, loc, max_paths=None):
        paths = []
        for dirpath, dirnames, filenames in os.walk(loc):
            for filename in filenames:
                paths.append("{}/{}".format(dirpath, filename))
                if max_paths and len(paths) >= max_paths:
                    return paths
        return paths

    def load(self, path):
        with open(path, "rb") as f:
            bytes = f.read()
            return bytes, path
