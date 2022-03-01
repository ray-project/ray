package io.ray.serve.api;

import com.google.common.base.Preconditions;
import io.ray.serve.DeploymentConfig;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

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

  protected Deployment(
      String deploymentDef,
      String name,
      DeploymentConfig config,
      String version,
      String prevVersion,
      Object[] initArgs,
      String routePrefix,
      Map<String, Object> rayActorOptions) {

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
        StringUtils.isBlank(version) || config.getAutoscalingConfig() == null,
        "Currently autoscaling is only supported for versioned deployments. Try @serve.deployment(version=...).");

    this.deploymentDef = deploymentDef;
    this.name = name;
    this.version = version;
    this.prevVersion = prevVersion;
    this.config = config;
    this.initArgs = initArgs;
    this.routePrefix = routePrefix;
    this.rayActorOptions = rayActorOptions;

    if (StringUtils.isNotBlank(routePrefix)) {
      this.url = Serve.getGlobalClient().getRootUrl() + routePrefix;
    }
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
}
