import argparse
import time

import torch
import torchvision
import torchvision.transforms as transforms
import torch.optim as optim
import torch.nn as nn

from models import SimpleCNN

def prepare_imagenet(imagenet_path, imgsize = 256):
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Resize((imgsize, imgsize)),
    ])
    trainset = torchvision.datasets.ImageFolder(root=imagenet_path, transform=transform)
    return trainset

def download_dataset(imgsize):
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Resize((imgsize, imgsize)),
    ])
    trainset = torchvision.datasets.CIFAR10(root='./data', train=True, download=True, transform=transform)
    testset = torchvision.datasets.CIFAR10(root='./data', train=False, download=True, transform=transform)
    return trainset, testset

def parse_args():
    parser = argparse.ArgumentParser(description='Pytorch CUDA benchmark. Loads CIFAR10 and runs 3 training epochs.')
    parser.add_argument(
        '--pin',
        help='Pins memory. Default is false.',
        action='store_true',
        required=False
    )
    parser.add_argument(
        '--model',
        help='Which model to train. Small is simple CNN, large is VGG11.',
        choices=['small', 'large'],
        default='large',
        required=False,
    )
    parser.add_argument(
        '--size',
        help='Width/height to scale the images to. Only applies to CIFAR10 dataset. Prefer a power of 2.',
        type=int,
        default=32,
        required=False,
    )
    parser.add_argument(
        '--dataset',
        help='Which dataset to use. Default is CIFAR10.',
        choices=['cifar10', 'imagenet'],
        default='cifar10',
        required=False
    )
    parser.add_argument(
        '--imagenetpath',
        help='Root path to imagenet. Only applies if the selected dataset is imagenet.',
        default='/mnt/cluster_storage/aviv/testnet/files',
        required=False
    )
    args = parser.parse_args()
    return args

def train_epoch(net, criterion, optimizer, trainloader, pinned):
    net.train()
    for i,data in enumerate(trainloader):
        inputs, labels = data[0].to(device, non_blocking=pinned), data[1].to(device, non_blocking=pinned)
        optimizer.zero_grad()
        
        out = net(inputs)
        loss = criterion(out, labels)
        loss.backward()
        optimizer.step()

def test(net, testloader, pinned):
    net.eval()
    total = torch.zeros(1, dtype=torch.int).to(device, non_blocking=pinned)
    correct = torch.zeros(1, dtype=torch.int).to(device, non_blocking=pinned)
    with torch.no_grad():
        for data in testloader:
            images, labels = data[0].to(device, non_blocking=pinned), data[1].to(device, non_blocking=pinned)
            outputs = net(images)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum()
    return correct.item() / total.item()

if __name__ == '__main__':
    NUM_WORKERS = 8
    BATCH_SIZE = 100
    EPOCHS = 5

    if not torch.cuda.is_available():
        raise Exception('No GPU detected, cannot perform benchmark')
    device = torch.device('cuda')
    print('Using device:', device)

    args = parse_args()
    testloader = None
    if args.dataset == 'imagenet':
        trainset = prepare_imagenet(args.imagenetpath)
    else:
        trainset, testset = download_dataset(args.size)
        testloader = torch.utils.data.DataLoader(testset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, pin_memory=args.pin)
    trainloader = torch.utils.data.DataLoader(trainset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, pin_memory=args.pin)
    print('Dataset Loaded!')

    start_time = time.time()

    size = 256 if args.dataset == "imagenet" else args.size
    print(f'Model: {args.model}, Dataset: {args.dataset}, Pinned: {args.pin}, Size: {size}')
    net = torchvision.models.vgg11() if args.model == 'large' else SimpleCNN(size)
    net.to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(net.parameters(), lr=0.001, momentum=0.9)
    for epoch in range(EPOCHS):
        train_epoch(net, criterion, optimizer, trainloader, args.pin)
        if testloader:
            accuracy = test(net, testloader, args.pin)
            print(f"Accuracy after epoch {epoch + 1}: {accuracy*100}%")
    end_time = time.time()
    print(f'total time: {end_time - start_time}s')
