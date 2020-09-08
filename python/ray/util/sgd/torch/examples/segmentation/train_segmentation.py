import datetime
import os
import time

import torch
import torch.utils.data
from filelock import FileLock
from torch import nn
import torchvision

import ray
from ray.util.sgd.torch.examples.segmentation.coco_utils import get_coco
import ray.util.sgd.torch.examples.segmentation.transforms as T
import ray.util.sgd.torch.examples.segmentation.utils as utils
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd import TorchTrainer

try:
    from apex import amp
except ImportError:
    amp = None


def get_dataset(name,
                image_set,
                transform,
                num_classes_only=False,
                download="auto"):
    def sbd(*args, **kwargs):
        return torchvision.datasets.SBDataset(
            *args, mode="segmentation", **kwargs)

    paths = {
        "voc": (os.path.expanduser("~/datasets01/VOC/060817/"),
                torchvision.datasets.VOCSegmentation, 21),
        "voc_aug": (os.path.expanduser("~/datasets01/SBDD/072318/"), sbd, 21),
        "coco": (os.path.expanduser("~/datasets01/COCO/022719/"), get_coco, 21)
    }
    p, ds_fn, num_classes = paths[name]
    if num_classes_only:
        return None, num_classes

    if download == "auto" and os.path.exists(p):
        download = False
    try:
        ds = ds_fn(
            p, download=download, image_set=image_set, transforms=transform)
    except RuntimeError:
        print("data loading failed. Retrying this.")
        ds = ds_fn(p, download=True, image_set=image_set, transforms=transform)
    return ds, num_classes


def get_transform(train):
    base_size = 520
    crop_size = 480

    min_size = int((0.5 if train else 1.0) * base_size)
    max_size = int((2.0 if train else 1.0) * base_size)
    transforms = []
    transforms.append(T.RandomResize(min_size, max_size))
    if train:
        transforms.append(T.RandomHorizontalFlip(0.5))
        transforms.append(T.RandomCrop(crop_size))
    transforms.append(T.ToTensor())
    transforms.append(
        T.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]))

    return T.Compose(transforms)


def criterion(inputs, target):
    losses = {}
    for name, x in inputs.items():
        losses[name] = nn.functional.cross_entropy(x, target, ignore_index=255)

    if len(losses) == 1:
        return losses["out"]

    return losses["out"] + 0.5 * losses["aux"]


def get_optimizer(model, aux_loss):
    params_to_optimize = [
        {
            "params": [
                p for p in model.backbone.parameters() if p.requires_grad
            ]
        },
        {
            "params": [
                p for p in model.classifier.parameters() if p.requires_grad
            ]
        },
    ]
    if aux_loss:
        params = [
            p for p in model.aux_classifier.parameters() if p.requires_grad
        ]
        params_to_optimize.append({"params": params, "lr": args.lr * 10})
    optimizer = torch.optim.SGD(
        params_to_optimize,
        lr=args.lr,
        momentum=args.momentum,
        weight_decay=args.weight_decay)
    return optimizer


class SegOperator(TrainingOperator):
    def setup(self, config):
        args = config["args"]
        # Create Data Loaders.
        with FileLock(".ray.lock"):
            # Within a machine, this code runs synchronously.
            dataset, num_classes = get_dataset(
                args.dataset, "train", get_transform(train=True))
            config["num_classes"] = num_classes
            dataset_test, _ = get_dataset(
                args.dataset, "val", get_transform(train=False))

        data_loader = torch.utils.data.DataLoader(
            dataset,
            batch_size=args.batch_size,
            num_workers=args.data_workers,
            collate_fn=utils.collate_fn,
            drop_last=True)

        data_loader_test = torch.utils.data.DataLoader(
            dataset_test,
            batch_size=1,
            num_workers=args.data_workers,
            collate_fn=utils.collate_fn)

        # Create model.
        model = torchvision.models.segmentation.__dict__[args.model](
            num_classes=config["num_classes"],
            aux_loss=args.aux_loss,
            pretrained=args.pretrained)
        if config["num_workers"] > 1:
            model = torch.nn.SyncBatchNorm.convert_sync_batchnorm(model)

        # Create optimizer.
        optimizer = get_optimizer(model, aux_loss=args.aux_loss)

        # Register components.
        self.model, self.optimizer = self.register(
            models=model,
            optimizers=optimizer,
            train_loader=data_loader,
            validation_loader=data_loader_test)

    def train_batch(self, batch, batch_info):
        image, target = batch
        image, target = image.to(self.device), target.to(self.device)
        output = self.model(image)
        loss = criterion(output, target)
        self.optimizer.zero_grad()
        if self.use_fp16 and amp:
            with amp.scale_loss(loss, self.optimizer) as scaled_loss:
                scaled_loss.backward()
        else:
            loss.backward()
        self.optimizer.step()
        lr = self.optimizer.param_groups[0]["lr"]
        return {"loss": loss.item(), "lr": lr, "num_samples": len(batch)}

    def validate(self, data_loader, info=None):
        self.model.eval()
        confmat = utils.ConfusionMatrix(self.config["num_classes"])
        with torch.no_grad():
            for image, target in data_loader:
                image, target = image.to(self.device), target.to(self.device)
                output = self.model(image)
                output = output["out"]

                confmat.update(target.flatten(), output.argmax(1).flatten())

            confmat.reduce_from_all_processes()
        return confmat


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    print(args)
    start_time = time.time()
    config = {"args": args, "num_workers": args.num_workers}
    trainer = TorchTrainer(
        training_operator_cls=SegOperator,
        use_tqdm=True,
        use_fp16=True,
        num_workers=config["num_workers"],
        config=config,
        use_gpu=torch.cuda.is_available())

    for epoch in range(args.epochs):
        trainer.train()
        confmat = trainer.validate(reduce_results=False)[0]
        print(confmat)
        state_dict = trainer.state_dict()
        state_dict.update(epoch=epoch, args=args)
        torch.save(state_dict,
                   os.path.join(args.output_dir, f"model_{epoch}.pth"))

    total_time = time.time() - start_time
    total_time_str = str(datetime.timedelta(seconds=int(total_time)))
    print(f"Training time {total_time_str}")


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(
        description="PyTorch Segmentation Training with RaySGD")
    parser.add_argument(
        "--address",
        required=False,
        default=None,
        help="the address to use for connecting to a Ray cluster.")
    parser.add_argument("--dataset", default="voc", help="dataset")
    parser.add_argument("--model", default="fcn_resnet101", help="model")
    parser.add_argument(
        "--aux-loss", action="store_true", help="auxiliar loss")
    parser.add_argument("--device", default="cuda", help="device")
    parser.add_argument("-b", "--batch-size", default=8, type=int)
    parser.add_argument(
        "-n", "--num-workers", default=1, type=int, help="GPU parallelism")
    parser.add_argument(
        "--epochs",
        default=30,
        type=int,
        metavar="N",
        help="number of total epochs to run")
    parser.add_argument(
        "--data-workers",
        default=16,
        type=int,
        metavar="N",
        help="number of data loading workers (default: 16)")
    parser.add_argument(
        "--lr", default=0.01, type=float, help="initial learning rate")
    parser.add_argument(
        "--momentum", default=0.9, type=float, metavar="M", help="momentum")
    parser.add_argument(
        "--wd",
        "--weight-decay",
        default=1e-4,
        type=float,
        metavar="W",
        help="weight decay (default: 1e-4)",
        dest="weight_decay")
    parser.add_argument("--output-dir", default=".", help="path where to save")
    parser.add_argument(
        "--pretrained",
        dest="pretrained",
        help="Use pre-trained models from the modelzoo",
        action="store_true",
    )

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    ray.init(address=args.address)
    main(args)
