import os
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple, Union

import numpy as np
import torch
from PIL import Image

from .utils import download_url
from .vision import VisionDataset


class PhotoTour(VisionDataset):
    """`Multi-view Stereo Correspondence <http://matthewalunbrown.com/patchdata/patchdata.html>`_ Dataset.

    .. note::

        We only provide the newer version of the dataset, since the authors state that it

            is more suitable for training descriptors based on difference of Gaussian, or Harris corners, as the
            patches are centred on real interest point detections, rather than being projections of 3D points as is the
            case in the old dataset.

        The original dataset is available under http://phototour.cs.washington.edu/patches/default.htm.


    Args:
        root (str or ``pathlib.Path``): Root directory where images are.
        name (string): Name of the dataset to load.
        transform (callable, optional): A function/transform that takes in a PIL image
            and returns a transformed version.
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.

    """

    urls = {
        "notredame_harris": [
            "http://matthewalunbrown.com/patchdata/notredame_harris.zip",
            "notredame_harris.zip",
            "69f8c90f78e171349abdf0307afefe4d",
        ],
        "yosemite_harris": [
            "http://matthewalunbrown.com/patchdata/yosemite_harris.zip",
            "yosemite_harris.zip",
            "a73253d1c6fbd3ba2613c45065c00d46",
        ],
        "liberty_harris": [
            "http://matthewalunbrown.com/patchdata/liberty_harris.zip",
            "liberty_harris.zip",
            "c731fcfb3abb4091110d0ae8c7ba182c",
        ],
        "notredame": [
            "http://icvl.ee.ic.ac.uk/vbalnt/notredame.zip",
            "notredame.zip",
            "509eda8535847b8c0a90bbb210c83484",
        ],
        "yosemite": ["http://icvl.ee.ic.ac.uk/vbalnt/yosemite.zip", "yosemite.zip", "533b2e8eb7ede31be40abc317b2fd4f0"],
        "liberty": ["http://icvl.ee.ic.ac.uk/vbalnt/liberty.zip", "liberty.zip", "fdd9152f138ea5ef2091746689176414"],
    }
    means = {
        "notredame": 0.4854,
        "yosemite": 0.4844,
        "liberty": 0.4437,
        "notredame_harris": 0.4854,
        "yosemite_harris": 0.4844,
        "liberty_harris": 0.4437,
    }
    stds = {
        "notredame": 0.1864,
        "yosemite": 0.1818,
        "liberty": 0.2019,
        "notredame_harris": 0.1864,
        "yosemite_harris": 0.1818,
        "liberty_harris": 0.2019,
    }
    lens = {
        "notredame": 468159,
        "yosemite": 633587,
        "liberty": 450092,
        "liberty_harris": 379587,
        "yosemite_harris": 450912,
        "notredame_harris": 325295,
    }
    image_ext = "bmp"
    info_file = "info.txt"
    matches_files = "m50_100000_100000_0.txt"

    def __init__(
        self,
        root: Union[str, Path],
        name: str,
        train: bool = True,
        transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        super().__init__(root, transform=transform)
        self.name = name
        self.data_dir = os.path.join(self.root, name)
        self.data_down = os.path.join(self.root, f"{name}.zip")
        self.data_file = os.path.join(self.root, f"{name}.pt")

        self.train = train
        self.mean = self.means[name]
        self.std = self.stds[name]

        if download:
            self.download()

        if not self._check_datafile_exists():
            self.cache()

        # load the serialized data
        self.data, self.labels, self.matches = torch.load(self.data_file, weights_only=True)

    def __getitem__(self, index: int) -> Union[torch.Tensor, Tuple[Any, Any, torch.Tensor]]:
        """
        Args:
            index (int): Index

        Returns:
            tuple: (data1, data2, matches)
        """
        if self.train:
            data = self.data[index]
            if self.transform is not None:
                data = self.transform(data)
            return data
        m = self.matches[index]
        data1, data2 = self.data[m[0]], self.data[m[1]]
        if self.transform is not None:
            data1 = self.transform(data1)
            data2 = self.transform(data2)
        return data1, data2, m[2]

    def __len__(self) -> int:
        return len(self.data if self.train else self.matches)

    def _check_datafile_exists(self) -> bool:
        return os.path.exists(self.data_file)

    def _check_downloaded(self) -> bool:
        return os.path.exists(self.data_dir)

    def download(self) -> None:
        if self._check_datafile_exists():
            print(f"# Found cached data {self.data_file}")
            return

        if not self._check_downloaded():
            # download files
            url = self.urls[self.name][0]
            filename = self.urls[self.name][1]
            md5 = self.urls[self.name][2]
            fpath = os.path.join(self.root, filename)

            download_url(url, self.root, filename, md5)

            print(f"# Extracting data {self.data_down}\n")

            import zipfile

            with zipfile.ZipFile(fpath, "r") as z:
                z.extractall(self.data_dir)

            os.unlink(fpath)

    def cache(self) -> None:
        # process and save as torch files
        print(f"# Caching data {self.data_file}")

        dataset = (
            read_image_file(self.data_dir, self.image_ext, self.lens[self.name]),
            read_info_file(self.data_dir, self.info_file),
            read_matches_files(self.data_dir, self.matches_files),
        )

        with open(self.data_file, "wb") as f:
            torch.save(dataset, f)

    def extra_repr(self) -> str:
        split = "Train" if self.train is True else "Test"
        return f"Split: {split}"


def read_image_file(data_dir: str, image_ext: str, n: int) -> torch.Tensor:
    """Return a Tensor containing the patches"""

    def PIL2array(_img: Image.Image) -> np.ndarray:
        """Convert PIL image type to numpy 2D array"""
        return np.array(_img.getdata(), dtype=np.uint8).reshape(64, 64)

    def find_files(_data_dir: str, _image_ext: str) -> List[str]:
        """Return a list with the file names of the images containing the patches"""
        files = []
        # find those files with the specified extension
        for file_dir in os.listdir(_data_dir):
            if file_dir.endswith(_image_ext):
                files.append(os.path.join(_data_dir, file_dir))
        return sorted(files)  # sort files in ascend order to keep relations

    patches = []
    list_files = find_files(data_dir, image_ext)

    for fpath in list_files:
        img = Image.open(fpath)
        for y in range(0, img.height, 64):
            for x in range(0, img.width, 64):
                patch = img.crop((x, y, x + 64, y + 64))
                patches.append(PIL2array(patch))
    return torch.ByteTensor(np.array(patches[:n]))


def read_info_file(data_dir: str, info_file: str) -> torch.Tensor:
    """Return a Tensor containing the list of labels
    Read the file and keep only the ID of the 3D point.
    """
    with open(os.path.join(data_dir, info_file)) as f:
        labels = [int(line.split()[0]) for line in f]
    return torch.LongTensor(labels)


def read_matches_files(data_dir: str, matches_file: str) -> torch.Tensor:
    """Return a Tensor containing the ground truth matches
    Read the file and keep only 3D point ID.
    Matches are represented with a 1, non matches with a 0.
    """
    matches = []
    with open(os.path.join(data_dir, matches_file)) as f:
        for line in f:
            line_split = line.split()
            matches.append([int(line_split[0]), int(line_split[3]), int(line_split[1] == line_split[4])])
    return torch.LongTensor(matches)
