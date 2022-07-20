# Managing Java Deployments

This section should help you:

- create, query, update and configure deployments
- configure resources of your deployments

:::{tip}
Get in touch with us if you're using or considering using [Ray Serve](https://docs.google.com/forms/d/1l8HT35jXMPtxVUtQPeGoe09VGp5jcvSv0TqPgyz6lGU).
:::

```{contents}

```

## Updating a Deployment

We can update the code and the configuration of a deployment and redeploy it. The following example first deploys a deployment using class `SimpleDeployment1`. After redeployment, the class of "my_deployment" deployment will be `SimpleDeployment2`.

```java
import io.ray.serve.api.Serve;

public class ManageDeployment {

  public class SimpleDeployment1 {

    public String call(String data) {
      return "SimpleDeployment1";
    }
  }

  public class SimpleDeployment2 {

    public String call(String data) {
      return "SimpleDeployment2";
    }
  }

  public void update() {
    Serve.deployment()
        .setName("my_deployment")
        .setDeploymentDef(SimpleDeployment1.class.getName())
        .setNumReplicas(1)
        .create()
        .deploy(true);

    Serve.deployment()
        .setName("my_deployment")
        .setDeploymentDef(SimpleDeployment2.class.getName())
        .setNumReplicas(1)
        .create()
        .deploy(true);
  }
}

```

## Configuring a Deployment

### Scaling Out

By specifying the `numReplicas` parameter, you can change the number of deployment replicas:

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class ManageDeployment {

  public void scaleOut() {
    // Create with a single replica.
    Deployment deployment =
        Serve.deployment()
            .setName("my_deployment")
            .setDeploymentDef(SimpleDeployment1.class.getName())
            .setNumReplicas(1)
            .create();
    deployment.deploy(true);

    // Scale up to 10 replicas.
    deployment.options().setNumReplicas(10).create().deploy(true);

    // Scale back down to 1 replica.
    deployment.options().setNumReplicas(1).create().deploy(true);
  }
}
```

### Resource Management (CPUs, GPUs)

Through the `rayActorOptions` parameter, you can set the resources of deployment, such as using one GPU:

```java
import io.ray.serve.api.Serve;

public class ManageDeployment {

  public void manageResouce() {
    Serve.deployment()
        .setName("my_deployment")
        .setDeploymentDef(SimpleDeployment1.class.getName())
        .setRayActorOptions(ImmutableMap.of("num_gpus", 1))
        .create()
        .deploy(true);
  }
}
```



