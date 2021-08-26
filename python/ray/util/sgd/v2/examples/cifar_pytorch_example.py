import os
import time
import urllib.request
import zipfile

import ray
import ray.util.sgd.v2 as sgd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.nn.parallel.distributed import DistributedDataParallel
from torch.optim import lr_scheduler
from torch.utils.data import DistributedSampler
from torchvision import datasets, transforms, models

from PIL import Image

def download_data():
    filename, _ = urllib.request.urlretrieve(
        "https://download.pytorch.org/tutorial/hymenoptera_data.zip",
        "data.zip")
    zipfile.ZipFile(filename).extractall()


data_dir = "hymenoptera_data"

if not os.path.exists(data_dir):
    download_data()


train_root_dir = os.path.join(data_dir, "train")
val_root_dir = os.path.join(data_dir, "val")

train_samples = datasets.ImageFolder(train_root_dir).samples
train_dataset = ray.data.from_items(train_samples)

val_samples = datasets.ImageFolder(val_root_dir).samples
val_dataset = ray.data.from_items(val_samples)

train_transform = transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

val_transform = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

# dataloader = torch.utils.data.DataLoader(datasets.ImageFolder(
#     train_root_dir, transform), batch_size=4, shuffle=True, num_workers=4)
#
# for inputs, labels in dataloader:
#     import pdb; pdb.set_trace()

def load_image(item, transform):
    path, target = item
    # open path as file to avoid ResourceWarning (https://github.com/python-pillow/Pillow/issues/835)
    with open(path, 'rb') as f:
        img = Image.open(f)
        sample = img.convert('RGB')
    return transform(sample), torch.tensor(target)

#dataset = dataset.map(load_image).repeat(times=2).random_shuffle()
train_dataset = train_dataset.map(lambda item: load_image(item,
                                                          train_transform)).limit(2)
val_dataset = val_dataset.map(lambda item: load_image(item,
                                                      val_transform)).limit(2)

data_transforms = {
    'train': transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ]),
    'val': transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ]),
}

data_dir = 'hymenoptera_data'
# image_datasets = {x: datasets.ImageFolder(os.path.join(data_dir, x),
#                                           data_transforms[x])
#                   for x in ['train', 'val']}
# dataloaders = {x: torch.utils.data.DataLoader(image_datasets[x], batch_size=4,
#                                              shuffle=True, num_workers=4)
#               for x in ['train', 'val']}
# dataset_sizes = {x: len(image_datasets[x]) for x in ['train', 'val']}
# class_names = image_datasets['train'].classes

image_datasets = {x: datasets.ImageFolder(os.path.join(data_dir, x),
                                              data_transforms[x])
                      for x in ['train', 'val']}
image_datasets = {k: torch.utils.data.random_split(v, [2, len(v)-2])[0] for
                  k, v in image_datasets.items()}

assert len(image_datasets["train"]) == 2
for item in image_datasets["train"]:
    print("Image Dataset", item)

for item in image_datasets["train"]:
    print("Image Dataset", item)


def train_model(config):
    since = time.time()
    num_epochs=2


    model = models.resnet18(pretrained=True)
    num_ftrs = model.fc.in_features
    # Here the size of each output sample is set to 2.
    # Alternatively, it can be generalized to nn.Linear(num_ftrs, len(class_names)).
    model.fc = nn.Linear(num_ftrs, 2)

    model = DistributedDataParallel(model)

    optimizer = optim.SGD(model.module.parameters(), lr=0.001, momentum=0.9)

    criterion = nn.CrossEntropyLoss()

    # last_checkpoint = sgd.load_checkpoint()
    # if last_checkpoint:
    #     model.load_state_dict(last_checkpoint["model"])
    #     optimizer.load_state_dict(last_checkpoint["optimizer"])
    #     epoch = last_checkpoint["epoch"]
    # else:
    #     epoch = 0

    epoch = 0

    #model_ft = model_ft.to(config["device"])



    # Observe that all parameters are being optimized


    # Decay LR by a factor of 0.1 every 7 epochs
    scheduler = lr_scheduler.StepLR(optimizer, step_size=7,
                                           gamma=0.1)

    best_acc = 0.0

    for item in image_datasets["train"]:
        print("Image Dataset", item)
    samplers = {k: DistributedSampler(v) for k, v in image_datasets.items()}
    dataloaders = {
        x: torch.utils.data.DataLoader(image_datasets[x], batch_size=2,
                                       sampler=samplers[x])
        for x in ['train', 'val']}
    # dataset_sizes = {x: len(image_datasets[x]) for x in ['train', 'val']}

    for epoch in range(epoch, num_epochs):
        if sgd.world_rank() == 0:
            print('Epoch {}/{}'.format(epoch, num_epochs - 1))
            print('-' * 10)

        for phase in ["train", "val"]:
            if phase == "train":
                model.train()
            else:
                model.eval()

            running_loss = 0.0
            running_corrects = 0

            dataset = sgd.get_dataset_shard(phase).random_shuffle()
            print(dataset.count())
            #print(dataset_sizes[phase])
            # assert dataset.count() == dataset_sizes[phase], (dataset.count(
            #
            # ), dataset_sizes[phase])

            for inputs, labels in dataloaders[phase]:
                print("DataLoader size:", len(dataloaders))

            # Iterate over data.
            # for batch in dataset.iter_batches(batch_size=1):
            #     #print(batch)
            #     inputs = torch.stack([b[0] for b in batch])
            #     labels = torch.stack([b[1] for b in batch])
                print("Phase", phase, inputs)
                print("Input shape", inputs.shape)
                print("Label shape", labels.shape)
                #loader_input, loader_labels = next(dataloaders[phase])
                # inputs = inputs.to(device)
                # labels = labels.to(device)

                # zero the parameter gradients
                optimizer.zero_grad()

                # forward
                # track history if only in train
                with torch.set_grad_enabled(phase == 'train'):
                    outputs = model(inputs)
                    _, preds = torch.max(outputs, 1)
                    loss = criterion(outputs, labels)

                    # backward + optimize only if in training phase
                    if phase == 'train':
                        loss.backward()
                        optimizer.step()

                # statistics
                running_loss += loss.item() * inputs.size(0)
                running_corrects += torch.sum(preds == labels.data)

            if phase == 'train':
                scheduler.step()

            print(running_loss)
            print(running_corrects)
            epoch_loss = running_loss / dataset.count()
            epoch_acc = running_corrects.double() / dataset.count()

            if sgd.world_rank() == 0:
                print('{} Loss: {:.4f} Acc: {:.4f}'.format(
                    phase, epoch_loss, epoch_acc))

            # deep copy the model
            if phase == 'val' and epoch_acc > best_acc:
                best_acc = epoch_acc

        sgd.save_checkpoint(model=model.state_dict(),
                            optimizer=optimizer.state_dict(), epoch=epoch+1)


    time_elapsed = time.time() - since
    if sgd.world_rank() == 0:
        print('Training complete in {:.0f}m {:.0f}s'.format(
            time_elapsed // 60, time_elapsed % 60))
        print('Best val Acc: {:4f}'.format(best_acc))

    return model.module

trainer = sgd.Trainer(backend="torch", num_workers=2)
trainer.start()
trainer.run(train_model, dataset={"train": train_dataset, "val": val_dataset})