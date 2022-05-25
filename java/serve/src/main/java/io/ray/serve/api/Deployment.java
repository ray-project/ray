package io.ray.serve.api;

import java.util.Map;

import com.google.common.base.Preconditions;

import io.ray.serve.DeploymentConfig;
import io.ray.serve.RayServeHandle;

/**
 * Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.
 * <p>Deployments should be created, retrieved, and updated using `@serve.deployment`,
 * `serve.get_deployment`, and `Deployment.options`, respectively.
 */
public class Deployment {

  private String deploymentDef;

  private String name;

  private DeploymentConfig config;

  private String version;

  private String prevVersion;

  private Object[] initArgs;

  private String routePrefix;

  private Map<String, Object> rayActorOptions;

  private String url;

  protected Deployment() {}

  protected Deployment(
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
        "Currently autoscaling is only supported for versioned deployments. Try @serve.deployment(version=...).");

    this.deploymentDef = deploymentDef;
    this.name = name;
    this.version = version;
    this.prevVersion = prevVersion;
    this.config = config;
    this.initArgs = initArgs != null ? initArgs : new Object[0];
    this.routePrefix = routePrefix != null ? routePrefix : "/" + name;
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
    Serve.getGlobalClient().deleteDeployment(name);
  }

  /**
   * Get a ServeHandle to this deployment to invoke it from Java.
   *
   * @return ServeHandle
   */
  public RayServeHandle getHandle() {
    return Serve.getGlobalClient().getHandle(name, true);
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public Deployment setDeploymentDef(String deploymentDef) {
    this.deploymentDef = deploymentDef;
    return this;
  }

  public String getName() {
    return name;
  }

  public Deployment setName(String name) {
    this.name = name;
    return this;
  }

  public DeploymentConfig getConfig() {
    return config;
  }

  public void setConfig(DeploymentConfig config) {
    this.config = config;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getPrevVersion() {
    return prevVersion;
  }

  public void setPrevVersion(String prevVersion) {
    this.prevVersion = prevVersion;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public Deployment setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
    return this;
  }

  public String getRoutePrefix() {
    return routePrefix;
  }

  public Deployment setRoutePrefix(String routePrefix) {
    this.routePrefix = routePrefix;
    return this;
  }

  public Map<String, Object> getRayActorOptions() {
    return rayActorOptions;
  }

  public void setRayActorOptions(Map<String, Object> rayActorOptions) {
    this.rayActorOptions = rayActorOptions;
  }

  public Deployment setNumReplicas(int numReplicas) {
    this.initArgs = initArgs;
    return this;
  }
}
