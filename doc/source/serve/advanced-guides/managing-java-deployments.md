---
myst:
  html_meta:
    description: "Experimental Java API for creating, updating, configuring, and scaling Serve deployments, and calling Python deployments from Java."
---

(serve-java-api)=
# Experimental Java API

:::{warning}
The Java API is experimental and subject to change.

KubeRay doesn't currently support the Java API.
:::

Java is a mainstream programming language for production services. Ray Serve offers a native Java API for creating, updating, and managing deployments. You can create deployments in Java and call them from Python, or the reverse.

This page describes how to do the following:

- Create, query, and update Java deployments.
- Configure Java deployment resources.
- Manage Python deployments with the Java API.

```{contents}
```

## Create a deployment

To create and deploy a deployment of a class, pass the full name of the class as an argument to the `Serve.deployment()` method, as in the following example.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:start-after: docs-create-start
:end-before: docs-create-end
:language: java
```

## Access a deployment

After you deploy a deployment, you can fetch its instance by name.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:start-after: docs-query-start
:end-before: docs-query-end
:language: java
```

## Update a deployment

You can update a deployment's code and configuration and then redeploy it. The following example updates the `"counter"` deployment's initial value to 2.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:start-after: docs-update-start
:end-before: docs-update-end
:language: java
```

## Configure a deployment

You can configure your deployments to do the following:

- Scale out by increasing the number of [deployment replicas](serve-architecture-high-level-view).
- Assign [replica resources](serve-cpus-gpus) such as CPUs and GPUs.

The next two sections describe each configuration.

### Scale out

To change the number of deployment replicas, set the `numReplicas` parameter:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:start-after: docs-scale-start
:end-before: docs-scale-end
:language: java
```

(resource-management-cpus-gpus)=
### Manage CPU and GPU resources

To reserve resources for each deployment replica, such as one GPU, set the `rayActorOptions` parameter:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/ManageDeployment.java
:start-after: docs-resource-start
:end-before: docs-resource-end
:language: java
```

(managing-a-python-deployment)=
## Manage a Python deployment

You can also use the Java API to manage and call a Python deployment. For example, suppose you have a Python file `counter.py` in the `/path/to/code/` directory:

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
Before you call `Ray.init` or `Serve.start`, specify the directory that contains the Python code. For details, see [Cross-language programming](cross_language).
:::

## Roadmap

Ray Serve plans to provide more Java features, including the following:

- An improved Java API that matches the Python version
- HTTP ingress support
- Bring-your-own Java Spring project as a deployment
