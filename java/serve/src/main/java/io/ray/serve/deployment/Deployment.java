package io.ray.serve.deployment;

import com.google.common.base.Preconditions;
import io.ray.serve.api.Serve;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.handle.RayServeHandle;
import java.util.Map;

/**
 * Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.
 *
 * <p>Deployments should be created, retrieved, and updated using `Serve.deployment.create`,
 * `Serve.getDeployment`, and `Deployment.options.create`, respectively.
 */
public class Deployment {

  private final String deploymentDef;

  private final String name;

  private final DeploymentConfig config;

  private final String version;

  private final String prevVersion;

  private final Object[] initArgs;

  private final String routePrefix;

  private final Map<String, Object> rayActorOptions;

  private final String url;

  public Deployment(
      String deploymentDef,
      String name,
      DeploymentConfig config,
      String version,
      String prevVersion,
      Object[] initArgs,
      String routePrefix,
      Map<String, Object> rayActorOptions) {

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
        version != null || config.getAutoscalingConfig() == null,
        "Currently autoscaling is only supported for versioned deployments. Try Serve.deployment.setVersion.");

    this.deploymentDef = deploymentDef;
    this.name = name;
    this.version = version;
    this.prevVersion = prevVersion;
    this.config = config;
    this.initArgs = initArgs != null ? initArgs : new Object[0];
    this.routePrefix = routePrefix;
    this.rayActorOptions = rayActorOptions;
    this.url = routePrefix != null ? Serve.getGlobalClient().getRootUrl() + routePrefix : null;
  }

  /**
   * Deploy or update this deployment.
   *
   * @param blocking
   */
  public void deploy(boolean blocking) {
    Serve.getGlobalClient()
        .deploy(
            name,
            deploymentDef,
            initArgs,
            rayActorOptions,
            config,
            version,
            prevVersion,
            routePrefix,
            url,
            blocking);
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
        .setDeploymentDef(this.deploymentDef)
        .setName(this.name)
        .setVersion(this.version)
        .setPrevVersion(this.prevVersion)
        .setNumReplicas(this.config.getNumReplicas())
        .setInitArgs(this.initArgs)
        .setRoutePrefix(this.routePrefix)
        .setRayActorOptions(this.rayActorOptions)
        .setUserConfig(this.config.getUserConfig())
        .setMaxConcurrentQueries(this.config.getMaxConcurrentQueries())
        .setAutoscalingConfig(this.config.getAutoscalingConfig())
        .setGracefulShutdownWaitLoopS(this.config.getGracefulShutdownWaitLoopS())
        .setGracefulShutdownTimeoutS(this.config.getGracefulShutdownTimeoutS())
        .setHealthCheckPeriodS(this.config.getHealthCheckPeriodS())
        .setHealthCheckTimeoutS(this.config.getHealthCheckTimeoutS())
        .setLanguage(this.config.getDeploymentLanguage());
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public String getName() {
    return name;
  }

  public DeploymentConfig getConfig() {
    return config;
  }

  public String getVersion() {
    return version;
  }

  public String getPrevVersion() {
    return prevVersion;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public String getRoutePrefix() {
    return routePrefix;
  }

  public Map<String, Object> getRayActorOptions() {
    return rayActorOptions;
  }

  public String getUrl() {
    return url;
  }
}
