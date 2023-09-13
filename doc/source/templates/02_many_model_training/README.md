# Scaling Many Model Training with Ray Tune

| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This template demonstrates how to parallelize the training of hundreds of time-series forecasting models with [Ray Tune](https://docs.ray.io/en/latest/tune/index.html). The template uses the `statsforecast` library to fit models to partitions of the M4 forecasting competition dataset. |
| Time to Run | Around 5 minutes to train all models. |
| Minimum Compute Requirements | No hard requirements. The default is 8 nodes with 8 CPUs each. |
| Cluster Environment | This template uses the latest Anyscale-provided Ray ML image using Python 3.9: [`anyscale/ray-ml:latest-py39-gpu`](https://docs.anyscale.com/reference/base-images/overview), with some extra requirements from `requirements.txt` installed on top. If you want to change to a different cluster environment, make sure that it is based off of this image and includes all packages listed in the `requirements.txt` file. |

## Getting Started

**When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `start.ipynb` file and follow the instructions there.**

The end result of the template is fitting multiple models on each dataset partition, then determining the best model based on cross-validation metrics. Then, using the best model, we can generate forecasts like the ones shown below:

![Forecasts](https://github-production-user-asset-6210df.s3.amazonaws.com/3887863/239091118-2413f399-4636-40cf-8b12-8d3ce15f5ce1.png)
