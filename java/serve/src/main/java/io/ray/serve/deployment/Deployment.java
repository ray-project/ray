package io.ray.serve.deployment;

import com.google.common.base.Preconditions;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;
import io.ray.serve.dag.ClassNode;
import io.ray.serve.dag.DAGNode;
import io.ray.serve.handle.DeploymentHandle;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.
 *
 * <p>Deployments should be created, retrieved, and updated using `Serve.deployment.create`,
 * `Serve.getDeployment`, and `Deployment.options.create`, respectively.
 */
public class Deployment {

  private static final Logger LOGGER = LoggerFactory.getLogger(Deployment.class);

  private final String name;

  private final DeploymentConfig deploymentConfig;

  private final ReplicaConfig replicaConfig;

  private final String version;

  private String routePrefix;

  private final String url;

  private boolean ingress;

  // TODO support placement group.

  public Deployment(
      String name,
      DeploymentConfig deploymentConfig,
      ReplicaConfig replicaConfig,
      String version,
      String routePrefix) {

    if (StringUtils.isNotBlank(routePrefix)) {
      Preconditions.checkArgument(routePrefix.startsWith("/"), "route_prefix must start with '/'.");
      Preconditions.checkArgument(
          routePrefix.equals("/") || !routePrefix.endsWith("/"),
          "route_prefix must not end with '/' unless it's the root.");
      Preconditions.checkArgument(
          !routePrefix.contains("{") && !routePrefix.contains("}"),
          "route_prefix may not contain wildcards.");
    }

    Preconditions.checkArgument(
        version != null || deploymentConfig.getAutoscalingConfig() == null,
        "Currently autoscaling is only supported for versioned deployments. Try Serve.deployment.setVersion.");

    this.name = name;
    this.version = version;
    this.deploymentConfig = deploymentConfig;
    this.replicaConfig = replicaConfig;
    this.routePrefix = routePrefix;
    this.url = routePrefix != null ? Serve.getGlobalClient().getRootUrl() + routePrefix : null;
  }

  /**
   * Get a ServeHandle to this deployment to invoke it from Java.
   *
   * @return ServeHandle
   * @deprecated {@value Constants#MIGRATION_MESSAGE}
   */
  @Deprecated
  public DeploymentHandle getHandle() {
    LOGGER.warn(Constants.MIGRATION_MESSAGE);
    return Serve.getGlobalClient().getDeploymentHandle(name, "", true);
  }

  /**
   * Return a copy of this deployment with updated options.
   *
   * <p>Only those options passed in will be updated, all others will remain unchanged from the
   * existing deployment.
   *
   * @return
   */
  public DeploymentCreator options() {

    return new DeploymentCreator()
        .setDeploymentDef(this.replicaConfig.getDeploymentDef())
        .setName(this.name)
        .setVersion(this.version)
        .setNumReplicas(this.deploymentConfig.getNumReplicas())
        .setInitArgs(this.replicaConfig.getInitArgs())
        .setRoutePrefix(this.routePrefix)
        .setRayActorOptions(this.replicaConfig.getRayActorOptions())
        .setUserConfig(this.deploymentConfig.getUserConfig())
        .setMaxOngoingRequests(this.deploymentConfig.getMaxOngoingRequests())
        .setAutoscalingConfig(this.deploymentConfig.getAutoscalingConfig())
        .setGracefulShutdownWaitLoopS(this.deploymentConfig.getGracefulShutdownWaitLoopS())
        .setGracefulShutdownTimeoutS(this.deploymentConfig.getGracefulShutdownTimeoutS())
        .setHealthCheckPeriodS(this.deploymentConfig.getHealthCheckPeriodS())
        .setHealthCheckTimeoutS(this.deploymentConfig.getHealthCheckTimeoutS())
        .setLanguage(this.deploymentConfig.getDeploymentLanguage());
  }

  /**
   * Bind the arguments to the deployment and return an Application.
   *
   * <p>The returned Application can be deployed using `serve.run` (or via config file) or bound to
   * another deployment for composition.
   *
   * @param args
   * @return
   */
  public Application bind(Object... args) {
    Map<String, Object> otherArgsToResolve = new HashMap<>();
    otherArgsToResolve.put("deployment_schema", this);
    otherArgsToResolve.put("is_from_serve_deployment", true);
    DAGNode dagNode =
        new ClassNode(
            replicaConfig.getDeploymentDef(),
            args,
            replicaConfig.getRayActorOptions(),
            otherArgsToResolve);
    return Application.fromInternalDagNode(dagNode);
  }

  public String getName() {
    return name;
  }

  public DeploymentConfig getDeploymentConfig() {
    return deploymentConfig;
  }

  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  public String getVersion() {
    return version;
  }

  public String getRoutePrefix() {
    return routePrefix;
  }

  public String getUrl() {
    return url;
  }

  public void setRoutePrefix(String routePrefix) {
    this.routePrefix = routePrefix;
  }

  public boolean isIngress() {
    return ingress;
  }

  public void setIngress(boolean ingress) {
    this.ingress = ingress;
  }
}
