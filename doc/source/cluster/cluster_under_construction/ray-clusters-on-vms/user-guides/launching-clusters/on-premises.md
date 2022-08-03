# Launching On-Premises Cluster

You might want to set up an on-premises Ray cluster, i.e., to run Ray on bare metal machines, or in a private cloud. We provide two ways to start an on-premises cluster.

* If you know all the nodes in advance and have ssh access to them, you should start the ray cluster using **cluster-launcher**.
* Alternatively, you can **manually set up** the Ray cluster by installing the Ray package and starting the Ray processes on each node. 

## Start Ray by cluster launcher

The Ray cluster launcher is part of the `ray` command line tool ([install ray](https://docs.ray.io/en/latest/ray-overview/installation.html)). It allows you to start, stop and attach to a running ray cluster using commands like the `ray up`, `ray down` and `ray attach`.
The ray cluster requires a cluster config file. The config specifies the nodes to start the cluster, how to ssh to those nodes and other cluster customizations. See the full example.yaml here.
Here is an example cluster.yaml

```

```

```

```

With the yaml file, you can start a ray cluster using

```

```

ray up example.yaml

```

```

You can also update the ray cluster with 
**Manually Set up a Ray Cluster**

