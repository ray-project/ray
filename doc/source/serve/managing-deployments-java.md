# Managing Deployments(Java)

This section should help you:

- create, query, update and configure deployments
- configure resources of your deployments

:::{tip}
Get in touch with us if you're using or considering using [Ray Serve](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU).
:::

```{contents}

```

## Updating a Deployment

We can update the code and the configuration of a deployment and redeploy it. The following example first deploys the deployment with one replica. After redeployment, the number of deployment replicas will become to 2:

```java
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setNumReplicas(1)
        .setInitArgs(new Object[] {10})
        .create()
        .deploy(true);

    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setNumReplicas(2)
        .setInitArgs(new Object[] {10})
        .create()
        .deploy(true);
```

## Configuring a Deployment

### Scaling Out

By specifying the `numReplicas` parameter, you can change the number of replicas of the deployment:

```java
Serve.deployment()
    .setName("counter")
    .setDeploymentDef(Counter.class.getName())
    .setNumReplicas(1)
    .setInitArgs(new Object[] {10})
    .create()
    .deploy(true);

// Scale up to 10 replicas
Serve.deployment()
    .setName("counter")
    .setNumReplicas(10)
    .create()
    .deploy(true);

// Scale back down to 1 replicas
Serve.deployment()
    .setName("counter")
    .setNumReplicas(1)
    .create()
    .deploy(true);
```

### Resource Management (CPUs, GPUs)

Through the `rayActorOptions` parameter, you can set the resources of deployment, such as using one GPU:

```java
Serve.deployment()
    .setName("counter")
    .setDeploymentDef(Counter.class.getName())
    .setRayActorOptions(ImmutableMap.of("num_gpus", 1))
    .setInitArgs(new Object[] {10})
    .create()
    .deploy(false);
```



