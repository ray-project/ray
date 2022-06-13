---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.6
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Convert existing PyTorch code to Ray AIR

If you already have working PyTorch code, you don't have to start from scratch to utilize the benefits of Ray Air. Instead, you can continue to use your existing code and incrementally add Ray AIR components as needed.

Some of the benefits you'll get by using Ray AIR with your existing PyTorch training code:

- Easy distributed data-parallel training on a cluster
- Automatic checkpointing/fault tolerance and result tracking
- Parallell data preprocessing
- Seamless integration with hyperparameter tuning
- Scalable batch prediction
- Scalable model serving

This tutorial will show you how to start with Ray AIR from your existing PyTorch training code. We will learn how to **distribute your training** and do **scalable batch prediction**.

+++

## The example code

The example code we'll be using is that of the [PyTorch quickstart tutorial](https://pytorch.org/tutorials/beginner/basics/quickstart_tutorial.html). This code trains a neural network classifier on the FashionMNIST dataset.

You can find the code we used for this tutorial [here on GitHub](https://github.com/pytorch/tutorials/blob/8dddccc4c69116ca724aa82bd5f4596ef7ad119c/beginner_source/basics/quickstart_tutorial.py).

+++

## Unmodified
Let's start with the unmodified code from the example. A thorough explanation of the parts is given in the full tutorial - we'll just focus on the code here.

We start with some imports:

```{code-cell} ipython3
import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor
```

Then we download the data:

```{code-cell} ipython3
# Download training data from open datasets.
training_data = datasets.FashionMNIST(
    root="data",
    train=True,
    download=True,
    transform=ToTensor(),
)

# Download test data from open datasets.
test_data = datasets.FashionMNIST(
    root="data",
    train=False,
    download=True,
    transform=ToTensor(),
)
```

We can now define the dataloaders:

```{code-cell} ipython3
batch_size = 64

# Create data loaders.
train_dataloader = DataLoader(training_data, batch_size=batch_size)
test_dataloader = DataLoader(test_data, batch_size=batch_size)
```

We can then define and instantiate the neural network:

```{code-cell} ipython3
# Get cpu or gpu device for training.
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using {device} device")

# Define model
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28*28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10)
        )

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits

model = NeuralNetwork().to(device)
print(model)
```

Define our optimizer and loss:

```{code-cell} ipython3
loss_fn = nn.CrossEntropyLoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-3)
```

And finally our training loop. Note that we renamed the function from `train` to `train_epoch` to avoid conflicts with the Ray Train module later (which is also called `train`):

```{code-cell} ipython3
def train_epoch(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset)
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        X, y = X.to(device), y.to(device)

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")
```

And while we're at it, here is our validation loop (note that we sneaked in a `return test_loss` statement and also renamed the function):

```{code-cell} ipython3
def test_epoch(dataloader, model, loss_fn):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    print(f"Test Error: \n Accuracy: {(100*correct):>0.1f}%, Avg loss: {test_loss:>8f} \n")
    return test_loss
```

Now we can trigger training and save a model:

```{code-cell} ipython3
epochs = 5
for t in range(epochs):
    print(f"Epoch {t+1}\n-------------------------------")
    train_epoch(train_dataloader, model, loss_fn, optimizer)
    test_epoch(test_dataloader, model, loss_fn)
print("Done!")
```

```{code-cell} ipython3
torch.save(model.state_dict(), "model.pth")
print("Saved PyTorch Model State to model.pth")
```

We'll cover the rest of the tutorial (loading the model and doing batch prediction) later!

+++

## Introducing a wrapper function (no Ray AIR, yet!)
The notebook-style from the tutorial is great for tutorials, but in your production code you probably wrapped the actual training logic in a function. So let's do this here, too.

Note that we do not add or alter any code here (apart from variable definitions) - we just take the loose bits of code in the current tutorial and put them into one function.

```{code-cell} ipython3
def train_func():
    batch_size = 64
    lr = 1e-3
    epochs = 5
    
    # Create data loaders.
    train_dataloader = DataLoader(training_data, batch_size=batch_size)
    test_dataloader = DataLoader(test_data, batch_size=batch_size)
    
    # Get cpu or gpu device for training.
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using {device} device")
    
    model = NeuralNetwork().to(device)
    print(model)
    
    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    
    for t in range(epochs):
        print(f"Epoch {t+1}\n-------------------------------")
        train_epoch(train_dataloader, model, loss_fn, optimizer)
        test_epoch(test_dataloader, model, loss_fn)

    print("Done!")
```

Let's see it in action again:

```{code-cell} ipython3
train_func()
```

The output should look very similar to the previous ouput.

+++

## Starting with Ray AIR: Distribute the training

As a first step, we want to distribute the training across multiple workers. For this we want to

1. Use data-parallel training by sharding the training data
2. Setup the model to communicate gradient updates across machines
3. Report the results back to Ray Train.


To facilitate this, we only need a few changes to the code:

1. We import Ray Train:

```python
import ray.train as train
```


2. We use a `config` dict to configure some hyperparameters (this is not strictly needed but good practice):

```python
def train_func(config: dict):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]
```

3. We dynamically adjust the worker batch size according to the number of workers:

```python
    batch_size_per_worker = batch_size // train.world_size()
```

4. We prepare the data loader for distributed data sharding:

```python
    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)
```

5. We prepare the model for distributed gradient updates:

```python
    model = train.torch.prepare_model(model)
```

Note that `train.torch.prepare_model()` also automatically takes care of setting up devices (e.g. GPU training) - so we can get rid of those lines in our current code!


6. We capture the validation loss and report it to Ray train:

```python
        test_loss = test(test_dataloader, model, loss_fn)
        train.report(loss=test_loss)
```

7. In the `train_epoch()` and `test_epoch()` functions we divide the `size` by the world size:

```python
    size = len(dataloader.dataset) // train.world_size()  # Divide by word size
```

8. In the `train_epoch()` function we can get rid of the device mapping. Ray Train does this for us:

```python
        # We don't need this anymore! Ray Train does this automatically:
        # X, y = X.to(device), y.to(device) 
```

That's it - you need less than 10 lines of Ray Train-specific code and can otherwise continue to use your original code.

Let's take a look at the resulting code. First the `train_epoch()` function (2 lines changed, and we also commented out the print statement):

```{code-cell} ipython3
def train_epoch(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset) // train.world_size()  # Divide by word size
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        # We don't need this anymore! Ray Train does this automatically:
        # X, y = X.to(device), y.to(device)  

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            # print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")
```

Then the `test_epoch()` function (1 line changed, and we also commented out the print statement):

```{code-cell} ipython3
def test_epoch(dataloader, model, loss_fn):
    size = len(dataloader.dataset) // train.world_size()  # Divide by word size
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    # print(f"Test Error: \n Accuracy: {(100*correct):>0.1f}%, Avg loss: {test_loss:>8f} \n")
    return test_loss
```

And lastly, the wrapping `train_func()` where we added 4 lines and modified 2 (apart from the config dict):

```{code-cell} ipython3
import ray.train as train


def train_func(config: dict):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]
    
    batch_size_per_worker = batch_size // train.world_size()
    
    # Create data loaders.
    train_dataloader = DataLoader(training_data, batch_size=batch_size_per_worker)
    test_dataloader = DataLoader(test_data, batch_size=batch_size_per_worker)
    
    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)
    
    model = NeuralNetwork()
    model = train.torch.prepare_model(model)
    
    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    
    for t in range(epochs):
        train_epoch(train_dataloader, model, loss_fn, optimizer)
        test_loss = test_epoch(test_dataloader, model, loss_fn)
        train.report(loss=test_loss)

    print("Done!")
```

Now we'll use Ray Train's TorchTrainer to kick off the training. Note that we can set the hyperparmameters here! In the `scaling_config` we can also configure how many parallel workers to use and if we want to enable GPU training or not.

```{code-cell} ipython3
from ray.train.torch import TorchTrainer


trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={"lr": 1e-3, "batch_size": 64, "epochs": 4},
    scaling_config={"num_workers": 2, "use_gpu": False},
)
result = trainer.fit()
print(f"Last result: {result.metrics}")
```

Great, this works! You're now training your model in parallel. You could now scale this up to more nodes and workers on your Ray cluster.

But there are a few improvements we can make to the code in order to get the most of the system. For one, we should enable **checkpointing** to get access to the trained model afterwards. Additionally, we should optimize the **data loading** to take place within the workers.

+++

### Enabling checkpointing to retrieve the model
Enabling checkpointing is pretty easy - we just need to call the `train.save_checkpoint()` API and pass the model state to it:

```python
    train.save_checkpoint(epoch=t, model=model.module.state_dict())
```

Note that the `model.module` part is needed because the model gets wrapped in `torch.nn.DistributedDataParallel`.

### Move the data loader to the training function

You may have noticed a warning: `Warning: The actor TrainTrainable is very large (52 MiB). Check that its definition is not implicitly capturing a large array or other object in scope. Tip: use ray.put() to put large objects in the Ray object store.`.

This is because we load the data outside the training function. Ray then serializes it to make it accessible to the remote tasks (that may get executed on a remote node!). This is not too bad with just 52 MB of data, but imagine this were a full image dataset - you wouldn't want to ship this around the cluster unnecessarily. Instead, you should move the dataset loading part into the `train_func()`. This will then download the data *to disk* once per machine and result in much more efficient data loading.

The result looks like this:

```{code-cell} ipython3
def load_data():
    # Download training data from open datasets.
    training_data = datasets.FashionMNIST(
        root="data",
        train=True,
        download=True,
        transform=ToTensor(),
    )

    # Download test data from open datasets.
    test_data = datasets.FashionMNIST(
        root="data",
        train=False,
        download=True,
        transform=ToTensor(),
    )
    return training_data, test_data


def train_func(config: dict):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]
    
    batch_size_per_worker = batch_size // train.world_size()
    
    training_data, test_data = load_data()  # <- this is new!
    
    # Create data loaders.
    train_dataloader = DataLoader(training_data, batch_size=batch_size_per_worker)
    test_dataloader = DataLoader(test_data, batch_size=batch_size_per_worker)
    
    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)
    
    model = NeuralNetwork()
    model = train.torch.prepare_model(model)
    
    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    
    for t in range(epochs):
        train_epoch(train_dataloader, model, loss_fn, optimizer)
        test_loss = test_epoch(test_dataloader, model, loss_fn)
        train.save_checkpoint(epoch=t, model=model.module.state_dict())  # <- this is new!
        train.report(loss=test_loss)

    print("Done!")
```

Let's train again:

```{code-cell} ipython3
trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={"lr": 1e-3, "batch_size": 64, "epochs": 4},
    scaling_config={"num_workers": 2, "use_gpu": False},
)
result = trainer.fit()
```

We can see our results here:

```{code-cell} ipython3
print(f"Last result: {result.metrics}")
print(f"Checkpoint: {result.checkpoint}")
```

## Loading the model for prediction
You may have noticed that we skipped one part of the original tutorial - loading the model and using it for inference. The original code looks like this (we've wrapped it in a function):

```{code-cell} ipython3
def predict_from_model(model):
    classes = [
        "T-shirt/top",
        "Trouser",
        "Pullover",
        "Dress",
        "Coat",
        "Sandal",
        "Shirt",
        "Sneaker",
        "Bag",
        "Ankle boot",
    ]

    model.eval()
    x, y = test_data[0][0], test_data[0][1]
    with torch.no_grad():
        pred = model(x)
        predicted, actual = classes[pred[0].argmax(0)], classes[y]
        print(f'Predicted: "{predicted}", Actual: "{actual}"')
```

We can use our saved model with the existing code to do prediction:

```{code-cell} ipython3
from ray.train.torch import load_checkpoint

model, _ = load_checkpoint(result.checkpoint, NeuralNetwork())

predict_from_model(model)
```

To predict more than one example, we can use a loop:

```{code-cell} ipython3
classes = [
    "T-shirt/top",
    "Trouser",
    "Pullover",
    "Dress",
    "Coat",
    "Sandal",
    "Shirt",
    "Sneaker",
    "Bag",
    "Ankle boot",
]

def predict_from_model(model, data):
    model.eval()
    with torch.no_grad():
        for x, y in data:
            pred = model(x)
            predicted, actual = classes[pred[0].argmax(0)], classes[y]
            print(f'Predicted: "{predicted}", Actual: "{actual}"')
```

```{code-cell} ipython3
predict_from_model(model, [test_data[i] for i in range(10)])
```

## Using Ray AIR for scalable batch prediction
However, we can also use Ray AIRs `BatchPredictor` class to do scalable prediction.

```{code-cell} ipython3
from ray.air import BatchPredictor
from ray.air.predictors.integrations.torch import TorchPredictor

batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, TorchPredictor, model=NeuralNetwork())
```

The Batch predictors work with Ray Datasets. Here we convert our test dataset into a Ray Dataset - note that this is not very efficient, and you can look at our [other tutorials](https://docs.ray.io/en/master/ray-air/examples/index.html) to see more efficient ways to generate a Ray Dataset.

```{code-cell} ipython3
import ray.data

ds = ray.data.from_items([x for x, y in test_data])
```

We can then trigger prediction with two workers:

```{code-cell} ipython3
results = batch_predictor.predict(ds, min_scoring_workers=2)
```

`results` is another Ray Dataset. We can use `results.to_pandas()` to see our prediction results:

```{code-cell} ipython3
results.to_pandas()
```

If we want to convert these predictions into class names (as in the original example), we can use a `map` function to do this:

```{code-cell} ipython3
predicted_classes = results.map_batches(
    lambda batch: [classes[pred.argmax(0)] for pred in batch["predictions"]], 
    batch_format="pandas")
```

To compare this with the actual labels, let's create a Ray dataset for these and zip it together with the predicted classes:

```{code-cell} ipython3
real_classes = ray.data.from_items([classes[y] for x, y in test_data])
merged = predicted_classes.zip(real_classes)
```

Let's examine our results:

```{code-cell} ipython3
merged.to_pandas()
```

## Summary

This tutorial demonstrated how to turn your existing PyTorch code into code you can use with Ray AIR.

We learned how to
- enable distributed training using Ray Train abstractions
- save and retrieve model checkpoints via Ray AIR
- load a model for batch prediction

In our [other examples](https://docs.ray.io/en/master/ray-air/examples/index.html) you can learn how to do more things with the Ray AIR API, such as **serving your model with Ray Serve** or **tune your hyperparameters with Ray Tune.** You can also learn how to **construct Ray Datasets** to leverage Ray AIR's **preprocessing** API.

We hope this tutorial gave you a good starting point to leverage Ray AIR. If you have any questions, suggestions, or run into any problems pelase reach out on [Discuss](https://discuss.ray.io/) or [GitHub](https://github.com/ray-project/ray)!
