<!--- These will get pulled into anyscale docs -->
# Ray Train and RayTurbo Train

[Ray Train](https://docs.ray.io/en/latest/train/train.html) is an open source library for distributed training and fine-tuning. Ray Train allows you to scale model training code from a single machine to a cluster of machines in the cloud, abstracting away the complexities of distributed computing. It's suitable for handling large models or large datasets.

RayTurbo Train simplifies and extends the open source Ray Train experience with distributed training, making ML developers more productive. RayTurbo Train features improve the price-performance ratio, production monitoring, and developer experience.

## Elastic training

RayTurbo Train supports elastic training, enabling jobs to seamlessly adapt to changes in resource availability. This behavior ensures continuous execution despite hardware failures or node preemptions, avoiding idle or wasted time. As more nodes become available, the cluster dynamically scales up to speed up training with more worker processes.

To enable elastic training, use `ScalingConfig.num_workers` to specify `(min_workers, max_workers)` as a tuple instead of a fixed worker group size.

```python
from ray.train.torch import TorchTrainer, ScalingConfig

def train_func():
    # Your training code here
    ...

# Elastic training with 1-10 workers
scaling_config = ScalingConfig(num_workers=(1, 10), use_gpu=True)

trainer = TorchTrainer(train_func, scaling_config=scaling_config)
trainer.fit()
```

RayTurbo always requests `max_workers` number of workers, but if it can't get all of them, it starts if `min_workers` is available.

If any failures happen, RayTurbo restarts with fewer workers. Then it attempts again to bring up to `max_workers` number of workers.

If the cluster gets nodes eventually, RayTurbo restarts with the new workers added to the group.

## Ray Train Dashboard

RayTurbo provides a purpose-built dashboard designed to streamline the debugging of Ray Train workloads. This dashboard enables users to gain deeper insights into individual workers progress, pinpoint stragglers, and eliminate bottlenecks for faster, more efficient training.
