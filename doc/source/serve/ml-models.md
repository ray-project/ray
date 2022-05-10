# Serving ML Models

This section should help you:

- batch requests to optimize performance
- serve multiple models by composing deployments
- serve multiple models by making ensemble deployments

```{contents}
```

(serve-batching)=

## Request Batching

You can also have Ray Serve batch requests for performance, which is especially important for some ML models that run on GPUs. In order to use this feature, you need to do the following two things:

1. Use `async def` for your request handling logic to process queries concurrently.
2. Use the `@serve.batch` decorator to batch individual queries that come into the replica. The method/function that's decorated should handle a list of requests and return a list of the same length.

```python
@serve.deployment(route_prefix="/increment")
class BatchingExample:
    def __init__(self):
        self.count = 0

    @serve.batch
    async def handle_batch(self, requests):
        responses = []
        for request in requests:
            responses.append(request.json())

        return responses

    async def __call__(self, request):
        return await self.handle_batch(request)

BatchingExample.deploy()
```

Please take a look at [Batching Tutorial](serve-batch-tutorial) for a deep
dive.

(serve-model-composition)=

## Model Composition

:::{note}
Serve recently added an experimental API for building deployment graphs of multiple models.
Please take a look at the [Deployment Graph API](serve-deployment-graph) and try it out!
:::

Ray Serve supports composing individually scalable models into a single model
out of the box. For instance, you can combine multiple models to perform
stacking or ensembles.

To define a higher-level composed model you need to do three things:

1. Define your underlying models (the ones that you will compose together) as
   Ray Serve deployments.
2. Define your composed model, using the handles of the underlying models
   (see the example below).
3. Define a deployment representing this composed model and query it!

In order to avoid synchronous execution in the composed model (e.g., it's very
slow to make calls to the composed model), you'll need to make the function
asynchronous by using an `async def`. You'll see this in the example below.

That's it. Let's take a look at an example:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_model_composition.py
```

(serve-model-ensemble)=

## Model Ensemble

Ray Serve supports creating different ensemble models

To define an ensemble of different models you need to do three things:

1. Define your underlying sub models (the ones that make up the ensemble) as
   Ray Serve deployments.
2. Define your ensemble model, using the handles of the underlying models
   (see the example below).
3. Define a deployment representing this ensemble model and query it!

In order to avoid synchronous execution in the ensemble model, you'll need to make
the function asynchronous by using an `async def`. In contrast to a composition model,
within an ensemble model, you want to call **all** sub models in parallel. This will be
achieved by sending all prediction calls to the sub models via async by using
`asyncio.wait()`. Each serve deployment used in an ensemble use case is independently
scalable via changing `num_replicas`.

That's it. Let's take a look at an example:

```{literalinclude} ../../../python/ray/serve/examples/doc/snippet_model_ensemble.py
```

## Integration with Model Registries

Ray Serve is flexible.  If you can load your model as a Python
function or class, then you can scale it up and serve it with Ray Serve.

For example, if you are using the
[MLflow Model Registry](https://www.mlflow.org/docs/latest/model-registry.html)
to manage your models, the following wrapper
class will allow you to load a model using its MLflow `Model URI`:

```python
import pandas as pd
import mlflow.pyfunc

@serve.deployment
class MLflowDeployment:
    def __init__(self, model_uri):
        self.model = mlflow.pyfunc.load_model(model_uri=model_uri)

    async def __call__(self, request):
        csv_text = await request.body() # The body contains just raw csv text.
        df = pd.read_csv(csv_text)
        return self.model.predict(df)

model_uri = "model:/my_registered_model/Production"
MLflowDeployment.deploy(model_uri)
```

To serve multiple different MLflow models in the same program, use the `name` option:

```python
MLflowDeployment.options(name="my_mlflow_model_1").deploy(model_uri)
```

:::{tip}
The above approach will work for any model registry, not just MLflow.
Namely, load the model from the registry in `__init__`, and forward the request to the model in `__call__`.
:::

For a complete hands-on and seamless integration with MLflow, try this self-contained example on your laptop.
But first install `mlflow`.

```bash
pip install mlflow
```

```python
# This brief example shows how to deploy models saved in a model registry such as
# MLflow to Ray Serve, using the simple Ray Serve deployment APIs. You can peruse
# the saved models' metrics and parameters in MLflow ui.
#
import json
import numpy as np
import pandas as pd
import requests
import os
import tempfile

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier
from mlflow.tracking import MlflowClient

from ray import serve
import mlflow


def create_and_save_model():
    # load Iris data
    iris_data = load_iris()
    data, target, target_names = (iris_data['data'],
                                  iris_data['target'],
                                  iris_data['target_names'])

    # Instantiate a model
    model = GradientBoostingClassifier()

    # Training and validation split
    np.random.shuffle(data), np.random.shuffle(target)
    train_x, train_y = data[:100], target[:100]
    val_x, val_y = data[100:], target[100:]

    # Create labels list as file
    LABEL_PATH = os.path.join(tempfile.gettempdir(), "iris_labels.json")
    with open(LABEL_PATH, "w") as f:
        json.dump(target_names.tolist(), f)

    # Train the model and save our label list as an MLflow artifact
    # mlflow.sklearn.autolog automatically logs all parameters and metrics during
    # the training.
    mlflow.sklearn.autolog()
    with mlflow.start_run() as run:
        model.fit(train_x, train_y)
        # Log label list as a artifact
        mlflow.log_artifact(LABEL_PATH, artifact_path="labels")
    return run.info.run_id

#
# Create our Ray Serve deployment class
#


@serve.deployment(route_prefix="/regressor")
class BoostingModel:
    def __init__(self, uri):
        # Load the model and label artifact from the local
        # Mlflow model registry as a PyFunc Model
        self.model = mlflow.pyfunc.load_model(model_uri=uri)

        # Download the artifact list of labels
        local_dir = "/tmp/artifact_downloads"
        if not os.path.exists(local_dir):
            os.mkdir(local_dir)
        client = MlflowClient()
        local_path = f"{client.download_artifacts(run_id, 'labels', local_dir)}/iris_labels.json"
        with open(local_path, "r") as f:
            self.label_list = json.load(f)

    async def __call__(self, starlette_request):
        payload = await starlette_request.json()
        print(f"Worker: received Starlette request with data: {payload}")

        # Get the input vector from the payload
        input_vector = [
            payload["sepal length"],
            payload["sepal width"],
            payload["petal length"],
            payload["petal width"],
        ]

        # Convert the input vector in a Pandas DataFrame for prediction since
        # an MLflow PythonFunc model, model.predict(...), takes pandas DataFrame
        prediction = self.model.predict(pd.DataFrame([input_vector]))[0]
        human_name = self.label_list[prediction]
        return {"result": human_name}


if __name__ == '__main__':

    # Train and save the model artifacts in MLflow.
    # Here our MLflow model registry is local file
    # directory ./mlruns
    run_id = create_and_save_model()

    # Start the Ray Serve instance
    serve.start()
    # Construct model uri to load the model from our model registry
    uri = f"runs:/{run_id}/model"
    # Deploy our model.
    BoostingModel.deploy(uri)

    # Send in a request for labels types virginica, setosa, versicolor
    sample_request_inputs = [{
        "sepal length": 6.3,
        "sepal width": 3.3,
        "petal length": 6.0,
        "petal width": 2.5},
        {
        "sepal length": 5.1,
        "sepal width": 3.5,
        "petal length": 1.4,
        "petal width": 0.2},
        {
        "sepal length": 6.4,
        "sepal width": 3.2,
        "petal length": 4.5,
        "petal width": 1.5},
    ]
    for input_request in sample_request_inputs:
        response = requests.get("http://localhost:8000/regressor",
                            json=input_request)
        print(response.text)

    print("Launch MLflow ui to see the model parameters, metrics, and artifacts: `mlflow ui` from current directory.")

    #output
    #{
    #   "result": "versicolor"
    #}
    #{
    #    "result": "virginica"
    #}
    #{
    #    "result": "setosa"
    #}
    #
    # Launch MLflow ui to see the model parameters, metrics, and artifacts: `mlflow ui` from current directory.
```

For an even more hands-off and seamless integration with MLflow, check out the
[Ray Serve MLflow deployment plugin](https://github.com/ray-project/mlflow-ray-serve).  A full
tutorial is available [here](https://github.com/mlflow/mlflow/tree/master/examples/ray_serve).

## Framework-Specific Tutorials

Ray Serve seamlessly integrates with popular Python ML libraries.
Below are tutorials with some of these frameworks to help get you started.

- [PyTorch Tutorial](serve-pytorch-tutorial)
- [Scikit-Learn Tutorial](serve-sklearn-tutorial)
- [Keras and Tensorflow Tutorial](serve-tensorflow-tutorial)
- [RLlib Tutorial](serve-rllib-tutorial)
