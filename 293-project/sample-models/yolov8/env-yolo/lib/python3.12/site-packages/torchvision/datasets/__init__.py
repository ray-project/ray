from ._optical_flow import FlyingChairs, FlyingThings3D, HD1K, KittiFlow, Sintel
from ._stereo_matching import (
    CarlaStereo,
    CREStereo,
    ETH3DStereo,
    FallingThingsStereo,
    InStereo2k,
    Kitti2012Stereo,
    Kitti2015Stereo,
    Middlebury2014Stereo,
    SceneFlowStereo,
    SintelStereo,
)
from .caltech import Caltech101, Caltech256
from .celeba import CelebA
from .cifar import CIFAR10, CIFAR100
from .cityscapes import Cityscapes
from .clevr import CLEVRClassification
from .coco import CocoCaptions, CocoDetection
from .country211 import Country211
from .dtd import DTD
from .eurosat import EuroSAT
from .fakedata import FakeData
from .fer2013 import FER2013
from .fgvc_aircraft import FGVCAircraft
from .flickr import Flickr30k, Flickr8k
from .flowers102 import Flowers102
from .folder import DatasetFolder, ImageFolder
from .food101 import Food101
from .gtsrb import GTSRB
from .hmdb51 import HMDB51
from .imagenet import ImageNet
from .imagenette import Imagenette
from .inaturalist import INaturalist
from .kinetics import Kinetics
from .kitti import Kitti
from .lfw import LFWPairs, LFWPeople
from .lsun import LSUN, LSUNClass
from .mnist import EMNIST, FashionMNIST, KMNIST, MNIST, QMNIST
from .moving_mnist import MovingMNIST
from .omniglot import Omniglot
from .oxford_iiit_pet import OxfordIIITPet
from .pcam import PCAM
from .phototour import PhotoTour
from .places365 import Places365
from .rendered_sst2 import RenderedSST2
from .sbd import SBDataset
from .sbu import SBU
from .semeion import SEMEION
from .stanford_cars import StanfordCars
from .stl10 import STL10
from .sun397 import SUN397
from .svhn import SVHN
from .ucf101 import UCF101
from .usps import USPS
from .vision import VisionDataset
from .voc import VOCDetection, VOCSegmentation
from .widerface import WIDERFace

__all__ = (
    "LSUN",
    "LSUNClass",
    "ImageFolder",
    "DatasetFolder",
    "FakeData",
    "CocoCaptions",
    "CocoDetection",
    "CIFAR10",
    "CIFAR100",
    "EMNIST",
    "FashionMNIST",
    "QMNIST",
    "MNIST",
    "KMNIST",
    "StanfordCars",
    "STL10",
    "SUN397",
    "SVHN",
    "PhotoTour",
    "SEMEION",
    "Omniglot",
    "SBU",
    "Flickr8k",
    "Flickr30k",
    "Flowers102",
    "VOCSegmentation",
    "VOCDetection",
    "Cityscapes",
    "ImageNet",
    "Caltech101",
    "Caltech256",
    "CelebA",
    "WIDERFace",
    "SBDataset",
    "VisionDataset",
    "USPS",
    "Kinetics",
    "HMDB51",
    "UCF101",
    "Places365",
    "Kitti",
    "INaturalist",
    "LFWPeople",
    "LFWPairs",
    "KittiFlow",
    "Sintel",
    "FlyingChairs",
    "FlyingThings3D",
    "HD1K",
    "Food101",
    "DTD",
    "FER2013",
    "GTSRB",
    "CLEVRClassification",
    "OxfordIIITPet",
    "PCAM",
    "Country211",
    "FGVCAircraft",
    "EuroSAT",
    "RenderedSST2",
    "Kitti2012Stereo",
    "Kitti2015Stereo",
    "CarlaStereo",
    "Middlebury2014Stereo",
    "CREStereo",
    "FallingThingsStereo",
    "SceneFlowStereo",
    "SintelStereo",
    "InStereo2k",
    "ETH3DStereo",
    "wrap_dataset_for_transforms_v2",
    "Imagenette",
)


# We override current module's attributes to handle the import:
# from torchvision.datasets import wrap_dataset_for_transforms_v2
# without a cyclic error.
# Ref: https://peps.python.org/pep-0562/
def __getattr__(name):
    if name in ("wrap_dataset_for_transforms_v2",):
        from torchvision.tv_tensors._dataset_wrapper import wrap_dataset_for_transforms_v2

        return wrap_dataset_for_transforms_v2

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
