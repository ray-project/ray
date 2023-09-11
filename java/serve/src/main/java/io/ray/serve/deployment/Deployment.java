package io.ray.serve.deployment;

import com.google.common.base.Preconditions;
import io.ray.serve.api.Serve;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;
import io.ray.serve.dag.ClassNode;
import io.ray.serve.dag.DAGNode;
import io.ray.serve.handle.RayServeHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.
 *
 * <p>Deployments should be created, retrieved, and updated using `Serve.deployment.create`,
 * `Serve.getDeployment`, and `Deployment.options.create`, respectively.
 */
public class Deployment {

  private final String name;

  private final DeploymentConfig deploymentConfig;

  private final ReplicaConfig replicaConfig;

  private final String version;

  private final String routePrefix;

  private final String url;

  // TODO placement group parameters.

  public Deployment(
      String name,
      DeploymentConfig deploymentConfig,
      ReplicaConfig replicaConfig,
      String version,
      String routePrefix) {

    if (routePrefix != null) {
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
   * Deploy or update this deployment.
   *
   * @param blocking
   */
  @Deprecated
  public void deploy(boolean blocking) {
    Serve.getGlobalClient()
        .deploy(name, replicaConfig, deploymentConfig, version, routePrefix, url, blocking);
  }

  /** Delete this deployment. */
  public void delete() {
    Serve.getGlobalClient().deleteDeployment(name, true);
  }

  /**
   * Get a ServeHandle to this deployment to invoke it from Java.
   *
   * @return ServeHandle
   */
  public RayServeHandle getHandle() {
    return Serve.getGlobalClient().getHandle(name, true);
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
        .setMaxConcurrentQueries(this.deploymentConfig.getMaxConcurrentQueries())
        .setAutoscalingConfig(this.deploymentConfig.getAutoscalingConfig())
        .setGracefulShutdownWaitLoopS(this.deploymentConfig.getGracefulShutdownWaitLoopS())
        .setGracefulShutdownTimeoutS(this.deploymentConfig.getGracefulShutdownTimeoutS())
        .setHealthCheckPeriodS(this.deploymentConfig.getHealthCheckPeriodS())
        .setHealthCheckTimeoutS(this.deploymentConfig.getHealthCheckTimeoutS())
        .setLanguage(this.deploymentConfig.getDeploymentLanguage());
  }

  public Application bind() {
    return bind(null);
  }

  public Application bind(Object firstArg, Object... otherArgs) {
  	Object[] args = null;
  	if (firstArg != null) {
  		if (otherArgs == null) {
        args = new Object[] {firstArg};
      } else {
        args = Stream.concat(Stream.of(firstArg), Arrays.stream(otherArgs)).toArray(Object[]::new);
  		}
  	}

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
}
