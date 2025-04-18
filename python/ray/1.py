#!/usr/bin/env python3
"""
dataloader_benchmark.py - A script to benchmark PyTorch DataLoader performance on image datasets.
"""

import argparse
import os
import time
import torch
import torch.utils.data
import torchvision.datasets as datasets
import torchvision.transforms as transforms
from tabulate import tabulate


def parse_args():
    parser = argparse.ArgumentParser(description='PyTorch DataLoader Benchmark')
    parser.add_argument('data_path', type=str, help='Path to the image folder')
    parser.add_argument('--batch-sizes', type=int, nargs='+', default=[128, 256],
                        help='Batch sizes to test (default: [32, 64, 128, 256])')
    parser.add_argument('--workers', type=int, nargs='+', default=[16, 32],
                        help='Number of workers to test (default: [16, 8, 1])')
    parser.add_argument('--warmup-batches', type=int, default=100,
                        help='Number of warmup batches before measuring (default: 100)')
    parser.add_argument('--measure-batches', type=int, default=500,
                        help='Number of batches to measure (default: 500)')
    parser.add_argument('--pin-memory', action='store_true', default=True,
                        help='Use pin_memory in DataLoader')
    parser.add_argument('--shuffle', action='store_true', default=False,
                        help='Shuffle the data batches')
    parser.add_argument('--image-size', type=int, default=224,
                        help='Size to resize images to (default: 224)')
    parser.add_argument('--prefetch-factor', type=int, default=2,
                        help='Number of batches loaded in advance by each worker')
    return parser.parse_args()


def get_transforms(image_size):
    """Returns standard image transformations."""
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
    return transforms.Compose([
        transforms.RandomResizedCrop(image_size),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        normalize,
    ])


def measure_dataloader_performance(data_path, batch_size, num_workers, warmup_batches,
                                 measure_batches, pin_memory, image_size,
                                 prefetch_factor, shuffle):
    """Measures dataloader performance with specific configuration."""

    # Set up device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Create dataset
    try:
        dataset = datasets.ImageFolder(data_path, get_transforms(image_size))
    except Exception as e:
        print(f"Error loading dataset from {data_path}: {e}")
        return None

    # Create dataloader
    data_loader = torch.utils.data.DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=pin_memory,
        prefetch_factor=prefetch_factor if num_workers > 0 else None,
        persistent_workers=True if num_workers > 0 else False
    )

    total_samples = len(dataset)

    # Calculate size of each image in MB
    # 3 channels, float32 (4 bytes), image_size x image_size
    image_size_mb = 3 * 4 * image_size * image_size / (1024 * 1024)

    print(f"\nConfiguration: batch_size={batch_size}, workers={num_workers}")
    print(f"Total samples: {total_samples}")

    # Warmup
    print("Running warmup batches...")
    batch_count = 0
    start_time = time.time()
    for images, _ in data_loader:
        if torch.cuda.is_available():
            images = images.to(device, non_blocking=True)
        batch_count += 1
        if batch_count >= warmup_batches:
            break
        if batch_count % max(1, warmup_batches // 10) == 0:
            print(f"Warmup batch {batch_count}/{warmup_batches}")
    warmup_time = time.time() - start_time
    print(f"Warmup time: {warmup_time:.2f}s")

    # Measurement
    print("Measuring performance...")
    batch_times = []
    batch_count = 0
    batch_start = time.time()

    for images, _ in data_loader:
        if torch.cuda.is_available():
            images = images.to(device, non_blocking=True)

        # Simulate a small computation to ensure GPU transfer completes
        if torch.cuda.is_available():
            _ = images.mean()

        batch_end = time.time()
        batch_times.append(batch_end - batch_start)

        batch_count += 1
        if batch_count >= measure_batches:
            break

        if batch_count % max(1, measure_batches // 10) == 0:
            print(f"Measuring batch {batch_count}/{measure_batches}")

        batch_start = time.time()

    # Calculate statistics
    total_time = sum(batch_times)
    total_samples_processed = batch_count * batch_size
    avg_samples_per_sec = total_samples_processed / total_time
    avg_batch_time = total_time / batch_count

    # Calculate throughput in MB/s
    throughput_mbs = (avg_samples_per_sec * image_size_mb)

    print(f"Time: {total_time:.2f}s, Throughput: {avg_samples_per_sec:.2f} samples/sec ({throughput_mbs:.2f} MB/s)")

    return {
        'batch_size': batch_size,
        'num_workers': num_workers,
        'total_time': total_time,
        'avg_samples_per_sec': avg_samples_per_sec,
        'avg_batch_time': avg_batch_time * 1000,  # Convert to ms
        'samples_per_batch': batch_size,
        'batches_per_sec': 1.0 / avg_batch_time,
        'throughput_mbs': throughput_mbs
    }


def main():
    args = parse_args()

    # Check if the data path exists
    if not os.path.exists(args.data_path):
        print(f"Error: Data path '{args.data_path}' does not exist.")
        return

    print(f"DataLoader Benchmark on: {args.data_path}")
    print(f"Device: {'GPU' if torch.cuda.is_available() else 'CPU'}")
    if torch.cuda.is_available():
        print(f"GPU: {torch.cuda.get_device_name(0)}")
    print(f"PyTorch version: {torch.__version__}")

    results = []

    # Test different configurations
    for batch_size in args.batch_sizes:
        for num_workers in args.workers:
            result = measure_dataloader_performance(
                args.data_path, batch_size, num_workers, args.warmup_batches,
                args.measure_batches, args.pin_memory, args.image_size,
                args.prefetch_factor, args.shuffle
            )
            if result:
                results.append(result)

    # Sort results by throughput (samples/sec)
    results.sort(key=lambda x: x['avg_samples_per_sec'], reverse=True)

    # Display results in a table
    headers = [
        'Batch Size', 'Workers', 'Total Time (s)',
        'Throughput (samples/s)', 'Throughput (MB/s)', 'Batch Time (ms)', 'Batches/sec'
    ]
    table_data = [
        [
            r['batch_size'], r['num_workers'], f"{r['total_time']:.2f}",
            f"{r['avg_samples_per_sec']:.2f}", f"{r['throughput_mbs']:.2f}",
            f"{r['avg_batch_time']:.2f}", f"{r['batches_per_sec']:.2f}"
        ] for r in results
    ]

    print("\nResults (sorted by throughput):")
    print(tabulate(table_data, headers=headers, tablefmt='grid'))

    # Print best configuration
    best = results[0]
    print("\nBest configuration:")
    print(f"Batch size: {best['batch_size']}")
    print(f"Workers: {best['num_workers']}")
    print(f"Throughput: {best['avg_samples_per_sec']:.2f} samples/sec ({best['throughput_mbs']:.2f} MB/s)")
    print(f"Average batch time: {best['avg_batch_time']:.2f} ms")
    print(f"Total time: {best['total_time']:.2f} s")


if __name__ == '__main__':
    main()
