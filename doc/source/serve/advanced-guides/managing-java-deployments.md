(serve-java-api)=
# Experimental Java API

Ray Serve supports deploying Java deployments or applications and provides native API support. This chapter introduces how to use the Java API.

```{contents}
```

## Deployment and Application

Deployments are the core concept in Ray Serve. With the `Serve.deployment` method, you can define a Java Class as a deployment, just like in Python. Then, the `bind` method can be used to obtain an application from this deployment. Finally, use the `Serve.run` method to deploy this application.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/MyFirstDeployment.java
:start-after: api-deployment-start
:end-before: api-deployment-end
:language: java
```

## DeploymentHandle (composing deployments)

When binding a deployment, other applications can be passed as parameters. Then, in the constructor, you can directly use the `DeploymentHandle` corresponding to these applications. Here is an example:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/Ingress.java
:start-after: api-composing-start
:end-before: api-composing-end
:language: java
```

## HTTP handling

For a deployed application, in addition to accessing it through the DeploymentHandle, you can also access it via HTTP. For example:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpIngress.java
:start-after: api-http-start
:end-before: api-http-end
:language: java
```
