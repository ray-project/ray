package io.ray.serve.deployment;

import io.ray.serve.config.DeploymentConfig;
import java.io.Serializable;
import java.util.Map;

public class DeploymentWrapper implements Serializable {

  private static final long serialVersionUID = -5366203408463039652L;

  private String name;

  private String deploymentDef;

  private DeploymentConfig deploymentConfig;

  private Object[] initArgs;

  private DeploymentVersion deploymentVersion;

  private Map<String, String> config;

  public String getName() {
    return name;
  }

  public DeploymentWrapper setName(String name) {
    this.name = name;
    return this;
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public DeploymentWrapper setDeploymentDef(String deploymentDef) {
    this.deploymentDef = deploymentDef;
    return this;
  }

  public DeploymentConfig getDeploymentConfig() {
    return deploymentConfig;
  }

  public DeploymentWrapper setDeploymentConfig(DeploymentConfig deploymentConfig) {
    this.deploymentConfig = deploymentConfig;
    return this;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public DeploymentWrapper setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
    return this;
  }

  public DeploymentVersion getDeploymentVersion() {
    return deploymentVersion;
  }

  public DeploymentWrapper setDeploymentVersion(DeploymentVersion deploymentVersion) {
    this.deploymentVersion = deploymentVersion;
    return this;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public DeploymentWrapper setConfig(Map<String, String> config) {
    this.config = config;
    return this;
  }
}
