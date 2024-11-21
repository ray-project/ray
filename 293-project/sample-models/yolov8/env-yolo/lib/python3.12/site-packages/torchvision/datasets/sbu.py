import os
from pathlib import Path
from typing import Any, Callable, Optional, Tuple, Union

from PIL import Image

from .utils import check_integrity, download_and_extract_archive, download_url
from .vision import VisionDataset


class SBU(VisionDataset):
    """`SBU Captioned Photo <http://www.cs.virginia.edu/~vicente/sbucaptions/>`_ Dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory of dataset where tarball
            ``SBUCaptionedPhotoDataset.tar.gz`` exists.
        transform (callable, optional): A function/transform that takes in a PIL image
            and returns a transformed version. E.g, ``transforms.RandomCrop``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        download (bool, optional): If True, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.
    """

    url = "https://www.cs.rice.edu/~vo9/sbucaptions/SBUCaptionedPhotoDataset.tar.gz"
    filename = "SBUCaptionedPhotoDataset.tar.gz"
    md5_checksum = "9aec147b3488753cf758b4d493422285"

    def __init__(
        self,
        root: Union[str, Path],
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = True,
    ) -> None:
        super().__init__(root, transform=transform, target_transform=target_transform)

        if download:
            self.download()

        if not self._check_integrity():
            raise RuntimeError("Dataset not found or corrupted. You can use download=True to download it")

        # Read the caption for each photo
        self.photos = []
        self.captions = []

        file1 = os.path.join(self.root, "dataset", "SBU_captioned_photo_dataset_urls.txt")
        file2 = os.path.join(self.root, "dataset", "SBU_captioned_photo_dataset_captions.txt")

        for line1, line2 in zip(open(file1), open(file2)):
            url = line1.rstrip()
            photo = os.path.basename(url)
            filename = os.path.join(self.root, "dataset", photo)
            if os.path.exists(filename):
                caption = line2.rstrip()
                self.photos.append(photo)
                self.captions.append(caption)

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        """
        Args:
            index (int): Index

        Returns:
            tuple: (image, target) where target is a caption for the photo.
        """
        filename = os.path.join(self.root, "dataset", self.photos[index])
        img = Image.open(filename).convert("RGB")
        if self.transform is not None:
            img = self.transform(img)

        target = self.captions[index]
        if self.target_transform is not None:
            target = self.target_transform(target)

        return img, target

    def __len__(self) -> int:
        """The number of photos in the dataset."""
        return len(self.photos)

    def _check_integrity(self) -> bool:
        """Check the md5 checksum of the downloaded tarball."""
        root = self.root
        fpath = os.path.join(root, self.filename)
        if not check_integrity(fpath, self.md5_checksum):
            return False
        return True

    def download(self) -> None:
        """Download and extract the tarball, and download each individual photo."""

        if self._check_integrity():
            print("Files already downloaded and verified")
            return

        download_and_extract_archive(self.url, self.root, self.root, self.filename, self.md5_checksum)

        # Download individual photos
        with open(os.path.join(self.root, "dataset", "SBU_captioned_photo_dataset_urls.txt")) as fh:
            for line in fh:
                url = line.rstrip()
                try:
                    download_url(url, os.path.join(self.root, "dataset"))
                except OSError:
                    # The images point to public images on Flickr.
                    # Note: Images might be removed by users at anytime.
                    pass
