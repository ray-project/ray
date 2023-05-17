# Scaling Many Model Training with Ray Tune

This template demonstrates how to parallelize the training of hundreds of time-series forecasting models with [Ray Tune](https://docs.ray.io/en/latest/tune/index.html).

The template uses the `statsforecast` library to fit models to partitions of the M4 forecasting competition dataset.

The end result of the template is fitting multiple models on each dataset partition, then determining the best model based on cross-validation metrics. Then, using the best model, we can generate forecasts like the ones shown below:

![Forecasts](https://github-production-user-asset-6210df.s3.amazonaws.com/3887863/239091118-2413f399-4636-40cf-8b12-8d3ce15f5ce1.png)

When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `many_model_training.ipynb` file and follow the instructions there.
