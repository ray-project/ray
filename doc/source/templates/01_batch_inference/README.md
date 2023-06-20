# Scaling Batch Inference with Ray Data

| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This template walks through GPU batch inference on an image dataset. |
| Time to Run | Less than 5 minutes to compute predictions on the dataset. |
| Minimum Compute Requirements | No hard requirements. The default is 4 nodes, each with 1 NVIDIA T4 GPU. |
| Cluster Environment | This template uses the latest Anyscale-provided Ray ML image using Python 3.9: [`anyscale/ray-ml:latest-py39-gpu`](https://docs.anyscale.com/reference/base-images/overview). If you want to change to a different cluster environment, make sure that it is based off of this image! |

## Getting Started

**When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `start.ipynb` file and follow the instructions there.**

By the end, we will have classified around 10k images with a PyTorch model.
