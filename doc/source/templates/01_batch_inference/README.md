# Scaling Batch Inference with Ray Data

This template is a quickstart to using [Ray
Data](https://docs.ray.io/en/latest/data/data.html) for batch
inference. Ray Data is one of many libraries under the [Ray AI
Runtime](https://docs.ray.io/en/latest/ray-air/getting-started.html).
See [this blog
post](https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets)
for more information on why and how you should perform batch inference
with Ray!

This template walks through GPU batch prediction on an image dataset
using a PyTorch model, but the framework and data format are there just
to help you build your own application!

At a high level, this template will:

1.  [Load your dataset using Ray
    Data.](https://docs.ray.io/en/latest/data/loading-data.html)
2.  [Preprocess your dataset before feeding it to your
    model.](https://docs.ray.io/en/latest/data/transforming-data.html)
3.  [Initialize your model and perform inference on a shard of your
    dataset with a remote
    actor.](https://docs.ray.io/en/latest/data/transforming-data.html#callable-class-udfs)
4.  [Save your prediction
    results.](https://docs.ray.io/en/latest/data/api/input_output.html)

Start coding by clicking on the Jupyter or VSCode icon above.