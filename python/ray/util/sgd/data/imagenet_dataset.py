from local_dataset import LocalDataset
from PIL import Image
import io
import numpy as np
import os
import sys


def pil_loader(raw_bytes):
    with io.BytesIO(raw_bytes) as f:
        img = Image.open(f)
        return img.convert("RGB")


# From torch.utils.data.ImageNet
def _find_classes(dir):
    """
    Finds the class folders in a dataset.

    Args:
        dir (string): Root directory path.

    Returns:
        tuple: (classes, class_to_idx) where classes are relative to (dir), and class_to_idx is a dictionary.

    Ensures:
        No class is a subdirectory of another.
    """
    if sys.version_info >= (3, 5):
        # Faster and available in Python 3.5 and above
        classes = [d.name for d in os.scandir(dir) if d.is_dir()]
    else:
        classes = [d for d in os.listdir(dir) if os.path.isdir(os.path.join(dir, d))]
    classes.sort()
    class_to_idx = {classes[i]: i for i in range(len(classes))}
    return classes, class_to_idx


def _load_val_sol(val_sol_path):
        assert val_sol_path != None, "Must supply the path of LOC_val_solution.csv"
        solutions = {}
        classes = set()
        with open(val_sol_path, 'r') as f:
            import csv
            csv_reader = csv.reader(f)
            _header = next(csv_reader)
            for id, sol in csv_reader:
                label = sol.split(' ')[0]
                solutions[id] = label
                classes.add(label)
        classes_list = sorted(classes)
        class_to_idx = {classes_list[i]: i for i in range(len(classes_list))}
        return solutions, classes_list, class_to_idx


class ImageNetDataset(LocalDataset):
    """
    An example implementation of a dataset using imagenet
    (which can be downloaded here https://www.kaggle.com/c/imagenet-object-localization-challenge/data)
    """

    def __init__(self, loc, split, val_sol_path=None, max_paths=None, transform=None):
        assert split in ["train", "validate"]
        super(ImageNetDataset, self).__init__(loc, max_paths=max_paths, transform=transform)
        self.split = split
        if self.split == "validate":
            self._id_label_map, self.classes, self.class_to_idx = _load_val_sol(val_sol_path)
        else:
            self.classes, self.class_to_idx = _find_classes(loc)


    def convert_data(self, data):
        img = pil_loader(data)
        return img


    def convert_label(self, label):
        file_name = label.split('/')[-1]
        if self.split == "validate":
            id = file_name.split('.')[0]
            label = self._id_label_map[id]
        else:
            label = file_name.split('_')[0]
        return self.class_to_idx[label]



