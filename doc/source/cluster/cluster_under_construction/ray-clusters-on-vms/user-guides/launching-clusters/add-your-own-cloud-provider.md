# Integrate with a cluster management system


Ray supports integrate with a new cluster management system / cloud providers. You and do that by implement `NodeProvider` interface (100 LOC) and register it in [node_provider.py](https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py). Contributions are welcome!
