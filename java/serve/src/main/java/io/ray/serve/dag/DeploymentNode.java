package io.ray.serve.dag;

import io.ray.serve.deployment.Deployment;
import io.ray.serve.handle.RayServeHandle;
import java.util.Map;

public class DeploymentNode extends DAGNode {

  private String appName;

  private Deployment deployment;

  private RayServeHandle rayServeHandle;

  public DeploymentNode(
      Deployment deployment,
      String appName,
      Object[] deploymentInitArgs,
      Map<String, Object> rayActorOptions,
      Map<String, Object> otherArgsToResolve) {
    super(deploymentInitArgs, rayActorOptions, otherArgsToResolve);
    this.appName = appName;
    this.deployment = deployment;
    this.rayServeHandle =
        new RayServeHandle(deployment.getName(), appName, null, null); // TODO RayServeHandle
  }

  @Override
  public DAGNode copyImpl(
      Object[] newArgs, Map<String, Object> newOptions, Map<String, Object> newOtherArgsToResolve) {
    return new DeploymentNode(deployment, appName, newArgs, newOptions, newOtherArgsToResolve);
  }

  public String getAppName() {
    return appName;
  }

  public Deployment getDeployment() {
    return deployment;
  }

  public RayServeHandle getRayServeHandle() {
    return rayServeHandle;
  }
}
