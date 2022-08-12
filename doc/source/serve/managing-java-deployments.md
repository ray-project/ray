# Managing Java Deployments

Java is one of the mainstream programming languages for production services. Ray Serve offers a native Java API for creating, updating, and managing deployments. You can create Ray Serve deployments using Java and call them via Python, or vice versa.

This section helps you:

- create, query, update, and configure Java deployments
- configure resources of your Java deployments
- manage Python deployments using Java API

```{contents}
```

## Creating a Deployment

By specifying the full name of the class as an argument to the `Serve.deployment()` method, as shown in the code below, you can create and deploy a deployment of the class.

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

You can update a deployment's code and configuration and then redeploy it. The following example updates `"counter"` deployment's initial value to 2.

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-update-end
:language: java
:start-after: docs-update-start
```

## Configuring a Deployment

Ray Serve lets you configure your deployments to:

- scale out by increasing the number of deployment replicas
- assign replica resources such as CPUs and GPUs.

The next two sections describe how to configure your deployments.

### Scaling Out

By specifying the `numReplicas` parameter, you can change the number of deployment replicas:

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-scale-end
:language: java
:start-after: docs-scale-start
```

### Resource Management (CPUs, GPUs)

Through the `rayActorOptions` parameter, you can reserve resources for each deployment replica, such as one GPU:

```{literalinclude} ../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:end-before: docs-resource-end
:language: java
:start-after: docs-resource-start
```

## Managing a Python Deployment

A Python deployment can also be managed and called by the Java API. Suppose you have a Python file `counter.py` in the `/path/to/code/` directory:

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

You can deploy it through the Java API and call it through a `RayServeHandle`:

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

:::{note}
Before `Ray.init` or `Serve.start`, you need to specify a directory to find the Python code. For details, please refer to [Cross-Language Programming](cross_language).
:::

## Future Roadmap

In the future, Ray Serve plans to provide more Java features, such as:
- an improved Java API that matches the Python version
- HTTP ingress support
- bring-your-own Java Spring project as a deployment
