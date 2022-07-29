# Managing Java Deployments

Java is one of the mainstream programming languages. Some users are used to using Java for daily development. Therefore, we provide the Ray Serve Java API. Through this Java API, users can manage the deployments written in Java. At the same time, Java API can also manage Python model deployments across languages.

This section should help you:

- create, query, update and configure Java deployments
- configure resources of your Java deployments
- manage Python deployments

```{contents}
```

## Creating a Deployment

By specifying the full name of the class, we can create and deploy a deployment of the class.

```java
import io.ray.serve.api.Serve;
import java.util.concurrent.atomic.AtomicInteger;

public class ManageDeployment {

  public static class Counter {

    private AtomicInteger value;

    public Counter(Integer value) {
      this.value = new AtomicInteger(value);
    }

    public String call(String delta) {
      return String.valueOf(value.addAndGet(Integer.valueOf(delta)));
    }
  }

  public void create() {
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setInitArgs(new Object[] {1})
        .setNumReplicas(2)
        .create()
        .deploy(true);
  }
}
```

## Querying a Deployment

A deployed deployment can be found by its name.

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class ManageDeployment {

  public void query() {
    Deployment deployment = Serve.getDeployment("counter");
  }
}
```

## Updating a Deployment

We can update the code and the configuration of a deployment and redeploy it. The following example updates the initial value of the deployment 'counter' to 2.

```java
import io.ray.serve.api.Serve;

public class ManageDeployment {

  public void update() {
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setInitArgs(new Object[] {2})
        .setNumReplicas(2)
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
    Deployment deployment = Serve.getDeployment("counter");

    // Scale up to 10 replicas.
    deployment.options().setNumReplicas(10).create().deploy(true);

    // Scale down to 1 replica.
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
    Map<String, Object> rayActorOptions = new HashMap<>();
    rayActorOptions.put("num_gpus", 1);
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setRayActorOptions(rayActorOptions)
        .create()
        .deploy(true);
  }
}
```

## Managing a Python Deployment

A python deployment can also be managed and called by the Java API. Suppose we have a python file `counter.py` in path `/path/to/code/`:

```python
from ray import serve

@serve.deployment
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value)

```

We deploy it as a deployment and call it through RayServeHandle:

```java
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.generated.DeploymentLanguage;
import java.io.File;

public class ManagePythonDeployment {

  public static void main(String[] args) {

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + "/path/to/code/");

    Serve.start(true, false, null, null);

    Deployment deployment =
        Serve.deployment()
            .setDeploymentLanguage(DeploymentLanguage.PYTHON)
            .setName("counter")
            .setDeploymentDef("counter.Counter")
            .setNumReplicas(1)
            .setInitArgs(new Object[] {"1"})
            .create();
    deployment.deploy(true);

    System.out.println(Ray.get(deployment.getHandle().method("increase").remote("2")));
  }
}

```

> NOTE: Before `Ray.init` or `Serve.start`, we need to set the directory to find the Python code. For details, please refer to [Cross-Language Programming](cross_language).

## Future Roadmap

In the future, we will provide more features on Ray Serve Java, such as:
- better Java API experience
- HTTP ingress for Ray Serve Java
- deploy a Java Spring project as a deployment

