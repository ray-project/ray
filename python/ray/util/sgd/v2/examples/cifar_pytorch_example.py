import os
import urllib.request
import zipfile

import ray
import ray.util.sgd.v2 as sgd
import torch
import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim import lr_scheduler
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

samples = datasets.ImageFolder(train_root_dir).samples
dataset = ray.data.from_items(samples)

transform = transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])

# dataloader = torch.utils.data.DataLoader(datasets.ImageFolder(
#     train_root_dir, transform), batch_size=4, shuffle=True, num_workers=4)
#
# for inputs, labels in dataloader:
#     import pdb; pdb.set_trace()

def load_image(item):
    path, target = item
    # open path as file to avoid ResourceWarning (https://github.com/python-pillow/Pillow/issues/835)
    with open(path, 'rb') as f:
        img = Image.open(f)
        sample = img.convert('RGB')
    return transform(sample), target

#dataset = dataset.map(load_image).repeat(times=2).random_shuffle()
dataset = dataset.map(load_image).random_shuffle()



def train_model(config):
    #since = time.time()
    num_epochs=2

    model = models.resnet18(pretrained=True)
    num_ftrs = model.fc.in_features
    # Here the size of each output sample is set to 2.
    # Alternatively, it can be generalized to nn.Linear(num_ftrs, len(class_names)).
    model.fc = nn.Linear(num_ftrs, 2)
    #model_ft = model_ft.to(config["device"])

    criterion = nn.CrossEntropyLoss()

    # Observe that all parameters are being optimized
    optimizer = optim.SGD(model.parameters(), lr=0.001, momentum=0.9)

    # Decay LR by a factor of 0.1 every 7 epochs
    scheduler = lr_scheduler.StepLR(optimizer, step_size=7,
                                           gamma=0.1)

    # Fault tolerance
    # state_dict = sgd.load_checkpoint()
    # model.load_state_dict(state_dict)

    best_acc = 0.0

    phase = "train"

    for epoch in range(num_epochs):
        print('Epoch {}/{}'.format(epoch, num_epochs - 1))
        print('-' * 10)

        model.train()
        running_loss = 0.0
        running_corrects = 0

        dataset = sgd.get_dataset_shard()

        # Iterate over data.
        for batch in dataset.iter_batches(batch_size=4):
            #import pdb; pdb.set_trace()
            inputs = [b[0] for b in batch]
            labels = torch.tensor([b[1] for b in batch])
            #import pdb; pdb.set_trace()
            inputs = torch.stack(inputs)
            #labels = torch.stack(labels)
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

        #     epoch_loss = running_loss / dataset_sizes[phase]
        #     epoch_acc = running_corrects.double() / dataset_sizes[phase]
        #
        #     print('{} Loss: {:.4f} Acc: {:.4f}'.format(
        #         phase, epoch_loss, epoch_acc))
        #
        #     # deep copy the model
        #     if phase == 'val' and epoch_acc > best_acc:
        #         best_acc = epoch_acc
        #         best_model_wts = copy.deepcopy(model.state_dict())
        #
        # print()

    #time_elapsed = time.time() - since
    # print('Training complete in {:.0f}m {:.0f}s'.format(
    #     time_elapsed // 60, time_elapsed % 60))
    # print('Best val Acc: {:4f}'.format(best_acc))

    # load best model weights
    #model.load_state_dict(best_model_wts)
    return model

trainer = sgd.Trainer(backend="torch")
trainer.start()
trainer.run(train_model, dataset=dataset)