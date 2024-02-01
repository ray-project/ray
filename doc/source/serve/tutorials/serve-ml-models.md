(serve-ml-models-tutorial)=

# Serve ML Models (Tensorflow, PyTorch, Scikit-Learn, others)

This guide shows how to train models from various machine learning frameworks and deploy them to Ray Serve.


See the [Key Concepts](serve-key-concepts) to learn more general information about Ray Serve.

:::::{tab-set} 

::::{tab-item} Keras and TensorFlow

This example trains and deploys a simple TensorFlow neural net.
In particular, it shows:

- How to train a TensorFlow model and load the model from your file system in your Ray Serve deployment.
- How to parse the JSON request and make a prediction.

Ray Serve is framework-agnostic--you can use any version of TensorFlow.
This tutorial uses TensorFlow 2 and Keras. You also need `requests` to send HTTP requests to your model deployment. If you haven't already, install TensorFlow 2 and requests by running:

```console
$ pip install "tensorflow>=2.0" requests
```

Open a new Python file called `tutorial_tensorflow.py`. First, import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Next, train a simple MNIST model using Keras.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_train_model_begin__
:end-before: __doc_train_model_end__
```

Next, define a `TFMnistModel` class that accepts HTTP requests and runs the MNIST model that you trained. The `@serve.deployment` decorator makes it a deployment object that you can deploy onto Ray Serve. Note that Ray Serve exposes the deployment over an HTTP route. By default, when the deployment receives a request over HTTP, Ray Serve invokes the `__call__` method.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_define_servable_begin__
:end-before: __doc_define_servable_end__
```

:::{note}
When you deploy and instantiate the `TFMnistModel` class, Ray Serve loads the TensorFlow model from your file system so that it can be ready to run inference on the model and serve requests later.
:::

Now that you've defined the Serve deployment, prepare it so that you can deploy it.

```{literalinclude} ../doc_code/tutorial_tensorflow.py
:start-after: __doc_deploy_begin__
:end-before: __doc_deploy_end__
```

:::{note}
`TFMnistModel.bind(TRAINED_MODEL_PATH)` binds the argument `TRAINED_MODEL_PATH` to the deployment and returns a `DeploymentNode` object, a wrapping of the `TFMnistModel` deployment object, that you can then use to connect with other `DeploymentNodes` to form a more complex [deployment graph](serve-model-composition).
:::

Finally, deploy the model to Ray Serve through the terminal.

```console
$ serve run tutorial_tensorflow:mnist_model
```

Next, query the model. While Serve is running, open a separate terminal window, and run the following in an interactive Python shell or a separate Python script:

```python
import requests
import numpy as np

resp = requests.get(
    "http://localhost:8000/", json={"array": np.random.randn(28 * 28).tolist()}
)
print(resp.json())
```

You should get an output like the following, although the exact prediction may vary:

```bash
{
 "prediction": [[-1.504277229309082, ..., -6.793371200561523]],
 "file": "/tmp/mnist_model.h5"
}
```
::::

::::{tab-item} PyTorch

This example loads and deploys a PyTorch ResNet model.
In particular, it shows:

- How to load the model from PyTorch's pre-trained Model Zoo.
- How to parse the JSON request, transform the payload and make a prediction.

This tutorial requires PyTorch and Torchvision. Ray Serve is framework agnostic and works with any version of PyTorch. You also need `requests` to send HTTP requests to your model deployment. If you haven't already, install them by running:

```console
$ pip install torch torchvision requests
```

Open a new Python file called `tutorial_pytorch.py`. First, import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_pytorch.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

Define a class `ImageModel` that parses the input data, transforms the images, and runs the ResNet18 model loaded from `torchvision`. The `@serve.deployment` decorator makes it a deployment object that you can deploy onto Ray Serve.  Note that Ray Serve exposes the deployment over an HTTP route. By default, when the deployment receives a request over HTTP, Ray Serve invokes the `__call__` method.

```{literalinclude} ../doc_code/tutorial_pytorch.py
:start-after: __doc_define_servable_begin__
:end-before: __doc_define_servable_end__
```

:::{note}
When you deploy and instantiate an `ImageModel` class, Ray Serve loads the ResNet18 model from `torchvision` so that it can be ready to run inference on the model and serve requests later.
:::

Now that you've defined the Serve deployment, prepare it so that you can deploy it.

```{literalinclude} ../doc_code/tutorial_pytorch.py
:start-after: __doc_deploy_begin__
:end-before: __doc_deploy_end__
```

:::{note}
`ImageModel.bind()` returns a `DeploymentNode` object, a wrapping of the `ImageModel` deployment object, that you can then use to connect with other `DeploymentNodes` to form a more complex [deployment graph](serve-model-composition).
:::

Finally, deploy the model to Ray Serve through the terminal.
```console
$ serve run tutorial_pytorch:image_model
```

Next, query the model. While Serve is running, open a separate terminal window, and run the following in an interactive Python shell or a separate Python script:

```python
import requests

ray_logo_bytes = requests.get(
    "https://raw.githubusercontent.com/ray-project/"
    "ray/master/doc/source/images/ray_header_logo.png"
).content

resp = requests.post("http://localhost:8000/", data=ray_logo_bytes)
print(resp.json())
```

You should get an output like the following, although the exact number may vary:

```bash
{'class_index': 919}
```
::::

::::{tab-item} Scikit-learn

This example trains and deploys a simple scikit-learn classifier.
In particular, it shows:

- How to load the scikit-learn model from file system in your Ray Serve definition.
- How to parse the JSON request and make a prediction.

Ray Serve is framework-agnostic. You can use any version of sklearn. You also need `requests` to send HTTP requests to your model deployment. If you haven't already, install scikit-learn and requests by running:

```console
$ pip install scikit-learn requests
```

Open a new Python file called `tutorial_sklearn.py`. Import Ray Serve and some other helpers.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_import_begin__
:end-before: __doc_import_end__
```

**Train a Classifier**

Next, train a classifier with the [Iris dataset](https://scikit-learn.org/stable/auto_examples/datasets/plot_iris_dataset.html).


First, instantiate a `GradientBoostingClassifier` loaded from scikit-learn.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_instantiate_model_begin__
:end-before: __doc_instantiate_model_end__
```

Next, load the Iris dataset and split the data into training and validation sets.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_data_begin__
:end-before: __doc_data_end__
```

Then, train the model and save it to a file.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_train_model_begin__
:end-before: __doc_train_model_end__
```

**Deploy with Ray Serve**

Finally, you're ready to deploy the classifier using Ray Serve.

Define a `BoostingModel` class that runs inference on the `GradientBoosingClassifier` model you trained and returns the resulting label. It's decorated with `@serve.deployment` to make it a deployment object so you can deploy it onto Ray Serve. Note that Ray Serve exposes the deployment over an HTTP route. By default, when the deployment receives a request over HTTP, Ray Serve invokes the `__call__` method.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_define_servable_begin__
:end-before: __doc_define_servable_end__
```

:::{note}
When you deploy and instantiate a `BoostingModel` class, Ray Serve loads the classifier model that you trained from the file system so that it can be ready to run inference on the model and serve requests later.
:::

After you've defined the Serve deployment, prepare it so that you can deploy it.

```{literalinclude} ../doc_code/tutorial_sklearn.py
:start-after: __doc_deploy_begin__
:end-before: __doc_deploy_end__
```

:::{note}
`BoostingModel.bind(MODEL_PATH, LABEL_PATH)` binds the arguments `MODEL_PATH` and `LABEL_PATH` to the deployment and returns a `DeploymentNode` object, a wrapping of the `BoostingModel` deployment object, that you can then use to connect with other `DeploymentNodes` to form a more complex [deployment graph](serve-model-composition).
:::

Finally, deploy the model to Ray Serve through the terminal.
```console
$ serve run tutorial_sklearn:boosting_model
```

Next, query the model. While Serve is running, open a separate terminal window, and run the following in an interactive Python shell or a separate Python script:

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

You should get an output like the following, although the exact prediction may vary:
```python
{"result": "versicolor"}
```

::::

:::::
