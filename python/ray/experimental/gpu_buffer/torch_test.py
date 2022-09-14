import argparse
import time

import torch
import torchvision
import torchvision.transforms as transforms
import torch.optim as optim
import torch.nn as nn

from models import SimpleCNN


def download_dataset():
    transform = transforms.Compose(
    [transforms.ToTensor(),
     transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])
    trainset = torchvision.datasets.CIFAR10(root='./data', train=True, download=True, transform=transform)
    testset = torchvision.datasets.CIFAR10(root='./data', train=False, download=True, transform=transform)
    return trainset, testset

def train_epoch(net, criterion, optimizer, trainloader):
    net.train()
    for i,data in enumerate(trainloader):
        inputs, labels = data[0].to(device), data[1].to(device)
        optimizer.zero_grad()
        
        out = net(inputs)
        loss = criterion(out, labels)
        loss.backward()
        optimizer.step()

def test(net, testloader):
    net.eval()
    total = 0
    correct = 0
    with torch.no_grad():
        for data in testloader:
            images, labels = data[0].to(device), data[1].to(device)
            outputs = net(images)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    return correct / total

def parse_args():
    parser = argparse.ArgumentParser(description='Pytorch CUDA benchmark. Loads CIFAR10 and runs 3 training epochs.')
    parser.add_argument(
        '--download-only',
        action='store_true',
        required=False
    )
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
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    NUM_WORKERS = 0
    BATCH_SIZE = 4
    EPOCHS = 3

    if not torch.cuda.is_available():
        raise Exception('No GPU detected, cannot perform benchmark')
    device = torch.device('cuda')
    print('Using device:', device)

    args = parse_args()
    trainset, testset = download_dataset()
    print('Dataset Downloaded!')
    if args.download_only:
        exit()

    start_time = time.time()
    trainloader = torch.utils.data.DataLoader(trainset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, pin_memory=args.pin)
    testloader = torch.utils.data.DataLoader(testset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, pin_memory=args.pin)
    print('Dataset Loaded!')

    print(f'Mode: {args.model}, Pin: {args.pin}')
    net = torchvision.models.vgg11() if args.model == 'large' else SimpleCNN()
    net.to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(net.parameters(), lr=0.001, momentum=0.9)
    for epoch in range(EPOCHS):
        train_epoch(net, criterion, optimizer, trainloader)
        accuracy = test(net, testloader)
        print(f"Accuracy after epoch {epoch + 1}: {accuracy*100}%")
    end_time = time.time()
    print(f'total time: {end_time - start_time}s')
