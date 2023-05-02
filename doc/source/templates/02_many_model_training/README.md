# Scaling Many Model Training with Ray Tune

This template is a quickstart to using [Ray
Tune](https://docs.ray.io/en/latest/tune/index.html) for batch
inference. Ray Tune is one of many libraries under the [Ray AI
Runtime](https://docs.ray.io/en/latest/ray-air/getting-started.html).
See [this blog
post](https://www.anyscale.com/blog/training-one-million-machine-learning-models-in-record-time-with-ray)
for more information on the benefits of performing many model training
with Ray!

This template walks through time-series forecasting using
`statsforecast`, but the framework and data format can be swapped out
easily \-- they are there just to help you build your own application!

At a high level, this template will:

1.  [Define the training function for a single partition of
    data.](https://docs.ray.io/en/latest/tune/tutorials/tune-run.html)
2.  [Define a Tune search space to run training over many partitions of
    data.](https://docs.ray.io/en/latest/tune/tutorials/tune-search-spaces.html)
3.  [Extract the best model per dataset partition from the Tune
    experiment
    output.](https://docs.ray.io/en/latest/tune/examples/tune_analyze_results.html)

Start coding by clicking on the Jupyter or VSCode icon above.
