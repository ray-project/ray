(serve-ml-models-tutorial)=

# Serving ML Models (Tensorflow, PyTorch, Scikit-Learn, others)

In this guide, we will show you how to train models from various machine learning frameworks and deploy them to Ray Serve.


Please see the [Key Concepts](serve-key-concepts) to learn more general information about Ray Serve.


::::{tabbed} Keras and Tensorflow


Let's train and deploy a simple Tensorflow neural net.
In particular, we will show:

- How to train a Tensorflow model and load the model from your file system in your Ray Serve deployment.
- How to parse the JSON request and make a prediction.

Ray Serve is framework agnostic -- you can use any version of Tensorflow.
However, for this tutorial, we will use Tensorflow 2 and Keras. We will also need `requests` to send HTTP requests to your model deployment. If you haven't already, please install Tensorflow 2 and requests by running:

```console
$ pip install "tensorflow>=2.0" requests
```

Open a new Python file called `tutorial_tensorflow.py`. First, let's import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Next, let's train a simple MNIST model using Keras.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_train_model_begin__
:end-before: __doc_train_model_end__
```

Next, we define a class `TFMnistModel` that will accept HTTP requests and run the MNIST model that we trained. It is decorated with `@serve.deployment` to make it a deployment object so it can be deployed onto Ray Serve. Note that the Serve deployment is exposed over an HTTP route, and by default the `__call__` method is invoked when a request is sent to your deployment over HTTP.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_define_servable_begin__
:end-before: __doc_define_servable_end__
```

:::{note} 
When `TFMnistModel` is deployed and instantiated, it will load the Tensorflow model from your file system so that it can be ready to run inference on the model and serve requests later.
:::

Now that we've defined our Serve deployment, let's prepare it so that it can be deployed.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_deploy_begin__
:end-before: __doc_deploy_end__
```

:::{note} 
`TFMnistModel.bind(TRAINED_MODEL_PATH)` binds the argument `TRAINED_MODEL_PATH` to our deployment and returns a `DeploymentNode` object (wrapping an `TFMnistModel` deployment object) that can then be used to connect with other `DeploymentNodes` to form a more complex [deployment graph](serve-model-composition-deployment-graph).
:::

Finally, we can deploy our model to Ray Serve through the terminal.
```console
$ serve run tutorial_tensorflow:mnist_model
```

Let's query it! While Serve is running, open a separate terminal window, and run the following in an interactive Python shell or a separate Python script:

```python
import requests
import numpy as np

resp = requests.get(
    "http://localhost:8000/", json={"array": np.random.randn(28 * 28).tolist()}
)
print(resp.json())
```

You should get an output like the following (the exact prediction may vary):

```bash
{
 "prediction": [[-1.504277229309082, ..., -6.793371200561523]],
 "file": "/tmp/mnist_model.h5"
}
```
::::

::::{tabbed} Pytorch

Let's load and deploy a PyTorch Resnet Model.
In particular, we will show:

- How to load the model from PyTorch's pre-trained modelzoo.
- How to parse the JSON request, transform the payload and make a prediction.

This tutorial will require PyTorch and Torchvision. Ray Serve is framework agnostic and works with any version of PyTorch. We will also need `requests` to send HTTP requests to your model deployment. If you haven't already, please install them by running:

```console
$ pip install torch torchvision requests
```

Open a new Python file called `tutorial_pytorch.py`. First, let's import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_pytorch.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

We define a class `ImageModel` that parses the input data, transforms the images, and runs the ResNet18 model loaded from `torchvision`. It is decorated with `@serve.deployment` to make it a deployment object so it can be deployed onto Ray Serve. Note that the Serve deployment is exposed over an HTTP route, and by default the `__call__` method is invoked when a request is sent to your deployment over HTTP.

```{literalinclude} ../doc_code/tutorial_pytorch.py
:start-after: __doc_define_servable_begin__
:end-before: __doc_define_servable_end__
```

:::{note} 
When `ImageModel` is deployed and instantiated, it will load the resnet18 model from `torchvision` so that it can be ready to run inference on the model and serve requests later.
:::

Now that we've defined our Serve deployment, let's prepare it so that it can be deployed.

```{literalinclude} ../doc_code/tutorial_pytorch.py
:start-after: __doc_deploy_begin__
:end-before: __doc_deploy_end__
```

:::{note} 
`ImageModel.bind()` returns a `DeploymentNode` object (wrapping an `ImageModel` deployment object) that can then be used to connect with other `DeploymentNodes` to form a more complex [deployment graph](serve-model-composition-deployment-graph).
:::

Finally, we can deploy our model to Ray Serve through the terminal.
```console
$ serve run tutorial_pytorch:image_model
```

Let's query it! While Serve is running, open a separate terminal window, and run the following in an interactive Python shell or a separate Python script:

```python
import requests

ray_logo_bytes = requests.get(
    "https://raw.githubusercontent.com/ray-project/"
    "ray/master/doc/source/images/ray_header_logo.png"
).content

resp = requests.post("http://localhost:8000/", data=ray_logo_bytes)
print(resp.json())
```

You should get an output like the following (the exact number may vary):

```bash
{'class_index': 919}
```
::::

::::{tabbed} Scikit-Learn

Let's train and deploy a simple Scikit-Learn classifier.
In particular, we will show:

- How to load the Scikit-Learn model from file system in your Ray Serve definition.
- How to parse the JSON request and make a prediction.

Ray Serve is framework agnostic. You can use any version of sklearn. We will also need `requests` to send HTTP requests to your model deployment. If you haven't already, please install scikit-learn and requests by running:

```console
$ pip install scikit-learn requests
```

Open a new Python file called `tutorial_sklearn.py`. Let's import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

**Train a Classifier**

We will train a classifier with the [iris dataset](https://scikit-learn.org/stable/auto_examples/datasets/plot_iris_dataset.html).


First, let's instantiate a `GradientBoostingClassifier` loaded from Scikit-Learn.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_instantiate_model_begin__
:end-before: __doc_instantiate_model_end__
```

Next, load the iris dataset and split the data into training and validation sets.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_data_begin__
:end-before: __doc_data_end__
```

We then train the model and save it to file.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_train_model_begin__
:end-before: __doc_train_model_end__
```

**Deploy with Ray Serve**

Finally, we are ready to deploy the classifier using Ray Serve!

We define a class `BoostingModel` that runs inference on the `GradientBoosingClassifier` model we trained and returns the resulting label. It is decorated with `@serve.deployment` to make it a deployment object so it can be deployed onto Ray Serve. Note that the Serve deployment is exposed over an HTTP route, and by default the `__call__` method is invoked when a request is sent to your deployment over HTTP.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_define_servable_begin__
:end-before: __doc_define_servable_end__
```

:::{note} 
When `BoostingModel` is deployed and instantiated, it will load the classifier model that we trained from your file system so that it can be ready to run inference on the model and serve requests later.
:::

Now that we've defined our Serve deployment, let's prepare it so that it can be deployed.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_deploy_begin__
:end-before: __doc_deploy_end__
```

:::{note} 
`BoostingModel.bind(MODEL_PATH, LABEL_PATH)` binds the arguments `MODEL_PATH` and `LABEL_PATH` to our deployment and returns a `DeploymentNode` object (wrapping an `BoostingModel` deployment object) that can then be used to connect with other `DeploymentNodes` to form a more complex [deployment graph](serve-model-composition-deployment-graph).
:::

Finally, we can deploy our model to Ray Serve through the terminal.
```console
$ serve run tutorial_sklearn:boosting_model
```

Let's query it! While Serve is running, open a separate terminal window, and run the following in an interactive Python shell or a separate Python script:

```python
import requests

sample_request_input = {
    "sepal length": 1.2,
    "sepal width": 1.0,
    "petal length": 1.1,
    "petal width": 0.9,
}
response = requests.get("http://localhost:8000/", json=sample_request_input)
print(response.text)
```

You should get an output like the following (the exact prediction may vary):
```python
{"result": "versicolor"}
```
::::