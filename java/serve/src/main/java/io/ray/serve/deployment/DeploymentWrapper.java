package io.ray.serve.deployment;

import java.io.Serializable;
import java.util.Map;

import io.ray.serve.config.DeploymentConfig;

public class DeploymentWrapper implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;

  private String deploymentDef;

  private DeploymentConfig deploymentConfig;

  private Object[] initArgs;

  private DeploymentVersion deploymentVersion;

  private Map<String, String> config;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public void setDeploymentDef(String deploymentDef) {
    this.deploymentDef = deploymentDef;
  }

  public DeploymentConfig getDeploymentConfig() {
    return deploymentConfig;
  }

  public void setDeploymentConfig(DeploymentConfig deploymentConfig) {
    this.deploymentConfig = deploymentConfig;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public void setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
  }

  public DeploymentVersion getDeploymentVersion() {
    return deploymentVersion;
  }

  public void setDeploymentVersion(DeploymentVersion deploymentVersion) {
    this.deploymentVersion = deploymentVersion;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }
}
