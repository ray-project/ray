SGD API Review
==============

There are a couple use cases and features we want to support for SGD.

1. As a user, I want to speed up my single-threaded model training code by distributed training.
2. As a user, I want to be able to feed in a large dataset (1TB) from the cloud into my distributed training scheme.
3. As a user, I want to be able to tune hyperparameters for any distributed training set up.
4. As a user, I want to be able to do elastic distributed training, (including with a large dataset [P1]).
5. As a user, I want to be able to develop/train my model on a jupyter notebook and validate it in the same environment.
6. As a user, I want to be able to integrate RaySGD in a way that is easily compatible with single-threaded code.
7. As a user, I want to be able to monitor the performance of my training mode.
8. As a user, I want my training to be completely reproducible.
6. [P1] As a user, I want to be able to have a plug-and-play solution for optimally speeding up my model training code / supporting large models.


### Non-supported use cases for Alpha 

1. Elastic training + large dataset (not yet, beta)
2. Supporting multiple datasets.

Below, I map out the APIs that meet each of the above requirements (R1, R2, R3, etc). 

General Model Training 
----------------------

Satisfies: R1, R5
Maybe satisfies: R6. 

Previous RaySGD API forced you to refactor your code. This is very high burden for users.

**One single long-running execution**
```python

def train_func(config):  # user always needs "config"
    model = Net()
    optimizer = torch.optimizer.SGD()
    model.cuda()
    DistributedDataParallel(...)

    # ray.sgd._local_rank() # not sure if we should expose
    torch.local_rank()  # if backend is pytorch
    hvd.cross_rank()  # if backend is horovod

    # Potentially add DistributedSampler here
    train_loader.sampler = DistributedSampler(...)
    for _ in config["epoch"]:
        train(model, optimizer, train_loader)
    return model, results


trainer = Trainer(num_workers=8, cpus_per_worker=2, gpus_per_worker=2, backend_args=TorchConfig())
print(trainer)
# prints a table of resource usage

model, result = trainer.run(train_func, config={...})
trainer.shutdown()
```



<details>
    <summary>Other considered options </summary>

Each of these do not play well with Ray Tune.
```python
# results = trainer.run(train_func, arg1=1, arg2=2)
# results = trainer.run(train_func, args=(arg1, arg2, arg3), kwargs={...})
# results = trainer.run(ray.partial(train_func, arg1=1, arg2=2))

```

</details>


Training with a Large dataset
-----------------------------

Satisfies: R2.

In this API, it's important to be able to modify the dataset / shard the dataset dynamically. Thus, split needs to be done in the worker rather than outside. 

A couple benefits to splitting the dataset dynamically:

1. Hyperparameter tuning -- you want to split the dataset differently per trial.
2. There's no forced ordering for dataset / trainer creation. (otherwise, you maybe to design your app to create data after instantiating your trainer, and maybe it's not so easy to do that.)

```python

    
# Preferred API
def train_func(config):
    batch_size = config["batch_size"]
    if ray.sgd.in_worker():
        dataloader = ray.sgd.get_dataset().get_split().to_torch(batch_size=batch_size)
    else:
        dataloader = get_dataset()
    ...
    return model

trainer = Trainer(num_workers=8, backend="torch")
dataset = ray.dataset.filter().pipeline(length=50) # do not call split!
trainer.run(train_func, config={}, dataset=dataset)

```

Cross Validation:

```python

cv_splits = ray.dataset.to_cv(splits=5)

for train_set, val_set in cv_splits:
    result = trainer.run(train_func, dataset=train_set)
    result = trainer.run(train_func, dataset=val_set)
```

<details>
<summary> Alternative API </summary>

```python
# This was not chosen because it requires the user to think about 
# too many points - both the training function signature and the Trainer declaration.

# def train_func(*args, dataset=None, **kwargs,):
#     if ray.sgd.in_worker():
#         dataloader = dataset.get_shard(self.rank).to_torch()
#     else:
#         dataset = ImageDataset("code")
#         dataloader = DataLoader(dataset)
#     for i, (x, y) in dataloader:
#         do_x_y(x, y)

# Alternative API: SPMD call.
# This was not chosen because it doesn't allow users to dynamically shard the dataset.
# # Trainer will make a SPMD call.
# model = trainer.map(
#     training_func, 
#     data=splits, 
#     arg1=[X for _ in trainer.workers], 
#     arg2=[Y for _ in trainer.workers])
# trainer.run(validation_func, model=model, dataset=val_dataset)


# Option 3:
# splits = ray.dataset.filter().split(locality_hint=trainer.workers)
# trainer.run(train_func, dataset=dataset)
```
</details>


Intermediate Reporting
----------------------

Satisfies: R3 (partially), R7.

Provide support for callbacks (which can be tensorboard, wandb etc). Also provides the same interface to use with Tune.

```python
def train_func(config):
    ...
    for _ in config["epochs"]:
        metrics = train()
        metrics = validate(...)
        ray.sgd.report(**metrics)
    return model

# Higher level API
results = trainer.run(train_func, config=config, callbacks=[...])

# Lower-level generator API
generator = trainer.run_generator(train_func, config=config)

for result in generator:
    do_stuff(result)

assert generator.is_finished()
model = generator.get_returns()


```



Hyperparameter Tuning
---------------------

Satisfies: R3.

Specific requirements for hyperparameter tuning.

1. Users should be able to do distributed tuning, training, and data processing.
2. Users should be able to tune the parameters of the dataset.
3. Going from SGD -> Tune should be easy.
4. Ability to do cross validation (1 dataset, shard multiple ways)

In the easy case, users can Tune it with a 2 line code change.

```python
def training_func(config):
    if ray.sgd.in_worker():
        dataloader = ray.sgd.get_dataset()\
            .get_shard(torch.rank())\
            .to_torch(batch_size=config["batch_size"])
    else:
        ...

    for i in epochs:
        ray.sgd.report(...)  # use same intermediate reporting API
    return model  # this is ignored in Tune?

# Declare the specification for training.
trainer = Trainer(gpus_per_worker=2, workers=12)
dataset = ray.dataset.pipeline()


# Convert this to a trainable. 

# trainer.run(train_func, config={}, dataset=dataset)

trainable = trainer.to_trainable(training_func, dataset=dataset)
analysis = tune.run(trainable, config={
    "lr": tune.uniform(), "batch_size": tune.randint(1, 2, 3)}, num_samples=12)

```

In the more complicated use case, you may want to do:

```python

def regular_training_code():
    pass


def tune_trainable(config):
    dataset = ray.dataset.pipeline(pipeline_args=config["args"])  # tune parameters of dataset
    trainer = Trainer(gpu_per_worker, workers=12)
    generator = trainer.run_generator(regular_training_code, dataset=dataset)
    for result in generator:
        tune.report(**result)

tune.run(tune_trainable, resources_per_trial={...})



# This API really sucks though because now you have to propagate resources back to TUne.
```

### Cross validation?


```python
# Inside Tune

split1, split2, split3 = dataset.pipeline.to_cv()

def training_func(config):
    for index, (train_set, val_set) in enumerate(ray.sgd.get_datasets()):
        dataloader = train_set.get_split().to_torch()
        for x in dataloader:
            ...
            sgd.report(cv=index, mean_loss=loss)
    return model

trainer.run(training_func, dataset=(split1, split2, split3))



```

<details> 
    <summary> Other alternatives</summary>


This API was not chosen for now. The Tune API is well established and we can introduce this later.

```python
trainer = Trainer(gpus_per_worker=4, num_workers=12)
best_model = trainer.hyperparameter_search(training_func, *tune_args)

```

</details>

Elasticity/Checkpointing
------------------------

Satisfies: R4.

1. Need to support variable number of workers
2. Doesn't need to to support hyperparameter tuning

```python
def train_func(*args)
    state = ray.sgd.load_checkpoint()
    # eventually, optional: 
    for _ in config["num_epochs"]:
        train(...)
        # optional, eventually: ray.sgd.report(...)
        ray.sgd.save_checkpoint((model, optimizer, etc), files=False|True)
        validate(...)
    return model

trainer = Trainer(gpus_per_worker, cpus_per_worker, num_workers=(4, inf))
trainer.run(train_func)
state = trainer.get_last_checkpoint()

```

TODO: how to do this with dataset?


Different allocation strategies / algorithmic improvements
----------------------------------------------------------


```python
def train_func(config)
    model = Net()
    optimizer = torch.sgd()
    ...

    model = ray.sgd.accelerate(model, loss, mixed_precision=True, etc)
    for _ in config["epoch"]:
        train(model, optimizer, train_loader)
    return model



```


Class API (Library developer use case)
--------------------------------------

Horovod API: https://github.com/ludwig-ai/ludwig/blob/eb4fa5ffad9b314d3cbea8069e0c3d82e5a74ad4/ludwig/backend/ray.py#L144

```python
trainer = Trainer(gpus_per_worker=4, num_workers=12, backend="horovod|pytorch")

trainer.start(ExecutionClass, args, kwargs)
trainer.execute(lambda w: w.train())
trainer.execute_single(lambda w: w.train())
trainer.shutdown()
```

Backends
--------

```python
trainer = Trainer(gpus_per_worker=4, num_workers=12, backend="horovod|pytorch")
result = trainer.run(...)

```


Alternative APIs
----------------

```python
worker_group = WorkerGroup()

model = ray.distributed_training(model, worker_group)
optimizer = Optimizer(model, worker_group)
dataset = ray.dataset.to_torch(locality_hint=worker_group)

for i, (input_batch, label_batch) in enumerate(dataset):
    # get the inputs; data is a list of [inputs, labels]
    # zero the parameter gradients
    optimizer.zero_grad()

    # forward + backward + optimize
    output_batch: ray.sgd.Batch = model(input_batch)
    # unfortunately "Tensor" is not a very pluggable abstraction
    loss = worker_group.apply(lambda output, labels: criterion(output_batch, label_batch))
    loss.backward()
    optimizer.step()

    # print statistics
    running_loss += loss.item()
    if i % 2000 == 1999:    # print every 2000 mini-batches
        print('[%d, %5d] loss: %.3f' %
              (epoch + 1, i + 1, running_loss / 2000))
        running_loss = 0.0

```

Notes about API decision
------------------------

### Why "config"?

* Tune compatibility -- one API. Avoids confusion about separating "tuning parameters" vs "function parameters".


### Why make the Trainer args





