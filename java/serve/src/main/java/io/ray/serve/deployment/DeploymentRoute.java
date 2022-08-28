package io.ray.serve.deployment;

import io.ray.serve.util.ServeProtoUtil;

public class DeploymentRoute {

  private final DeploymentInfo deploymentInfo;

  private final String route;

  public DeploymentRoute(DeploymentInfo deploymentInfo, String route) {
    this.deploymentInfo = deploymentInfo;
    this.route = route;
  }

  public DeploymentInfo getDeploymentInfo() {
    return deploymentInfo;
  }

  public String getRoute() {
    return route;
  }

  public static DeploymentRoute fromProto(io.ray.serve.generated.DeploymentRoute proto) {
    if (proto == null) {
      return null;
    }
    return new DeploymentRoute(
        DeploymentInfo.fromProto(proto.getDeploymentInfo()), proto.getRoute());
  }

  public static DeploymentRoute fromProtoBytes(byte[] bytes) {
    io.ray.serve.generated.DeploymentRoute proto =
        ServeProtoUtil.bytesToProto(bytes, io.ray.serve.generated.DeploymentRoute::parseFrom);
    return fromProto(proto);
  }
}
