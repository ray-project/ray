# Scaling Batch Inference with Ray Data

| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This template walks through GPU batch inference on an image dataset using a PyTorch ResNet model. |
| Time to Run | Less than 2 minutes to compute predictions on the dataset. |
| Minimum Compute Requirements | No hard requirements. The default is 4 nodes, each with 1 NVIDIA T4 GPU. |
| Cluster Environment | This template uses the latest Anyscale-provided Ray image using Python 3.9: [`anyscale/ray:latest-py39-gpu`](https://docs.anyscale.com/reference/base-images/ray-240/py39). |

## Getting Started

**When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `batch_inference.ipynb` file and follow the instructions there.**

By the end, we will have classified > 3000 images using the pre-trained ResNet model and saved these predictions to a local directory.
