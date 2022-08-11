# Managing Java Deployments

Java is one of the mainstream programming languages for production services. Ray Serve natively supports Java API for creating, updating, and managing deployments. You can create Ray Serve deployments using Java and call them via Python, or vice versa.

This section helps you to:

- create, query, update and configure Java deployments
- configure resources of your Java deployments
- manage Python deployments using Java API

```{contents}
```

## Creating a Deployment

By specifying the full name of the class as an argument to `Serve.deployment()` method, as shown in the code below, we can create and deploy our deployment of the class.

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-create-end
:language: java
:start-after: docs-create-start
```

## Accessing a Deployment

Once a deployment is deployed, you can fetch its instance by name.

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-query-end
:language: java
:start-after: docs-query-start
```

## Updating a Deployment

We can update the code and the configuration of a deployment and redeploy it. The following example updates the initial value of the deployment 'counter' to 2.

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-update-end
:language: java
:start-after: docs-update-start
```

## Configuring a Deployment

There are a couple of deployment configuration Serve supports:

- ability to scale out by increasing number of deployment replicas
- ability to assign resources such as CPU and GPUs.

The next two sections describe how to configure your deployments.

### Scaling Out

By specifying the `numReplicas` parameter, you can change the number of deployment replicas:

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-scale-end
:language: java
:start-after: docs-scale-start
```

### Resource Management (CPUs, GPUs)

Through the `rayActorOptions` parameter, you can set the resources of deployment, such as using one GPU:

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-resource-end
:language: java
:start-after: docs-resource-start
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

    Serve.start(true, false, null);

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
- improved API to match the Python version
- HTTP ingress support
- bring your own Java Spring project as a deployment

