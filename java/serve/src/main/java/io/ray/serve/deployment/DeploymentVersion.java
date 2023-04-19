package io.ray.serve.deployment;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.exception.RayServeException;
import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

public class DeploymentVersion implements Serializable {

  private static Gson gson = new Gson();

  private static final long serialVersionUID = 3400261981775851058L;

  private String codeVersion;

  private Object userConfig;

  private DeploymentConfig deploymentConfig;

  private Map<String, Object> rayActorOptions;

  private boolean unversioned;

  public DeploymentVersion() {
    this(null, new DeploymentConfig(), null);
  }

  public DeploymentVersion(String codeVersion) {
    this(codeVersion, new DeploymentConfig(), null);
  }

  public DeploymentVersion(
      String codeVersion, DeploymentConfig deploymentConfig, Map<String, Object> rayActorOptions) {
    if (StringUtils.isBlank(codeVersion)) {
      this.unversioned = true;
      this.codeVersion = RandomStringUtils.randomAlphabetic(6);
    } else {
      this.codeVersion = codeVersion;
    }
    if (deploymentConfig == null) {
      deploymentConfig = new DeploymentConfig();
    }
    this.deploymentConfig = deploymentConfig;
    this.rayActorOptions = rayActorOptions;
    this.userConfig = deploymentConfig.getUserConfig();
  }

  public String getCodeVersion() {
    return codeVersion;
  }

  public Object getUserConfig() {
    return userConfig;
  }

  public DeploymentConfig getDeploymentConfig() {
    return deploymentConfig;
  }

  public Map<String, Object> getRayActorOptions() {
    return rayActorOptions;
  }

  public boolean isUnversioned() {
    return unversioned;
  }

  public static DeploymentVersion fromProtoBytes(byte[] bytes) {
    if (bytes == null) {
      return new DeploymentVersion();
    }

    io.ray.serve.generated.DeploymentVersion proto = null;
    try {
      proto = io.ray.serve.generated.DeploymentVersion.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse DeploymentVersion from protobuf bytes.", e);
    }
    if (proto == null) {
      return new DeploymentVersion();
    }
    return new DeploymentVersion(
        proto.getCodeVersion(),
        DeploymentConfig.fromProto(proto.getDeploymentConfig()),
        gson.fromJson(proto.getRayActorOptions(), Map.class));
  }

  public byte[] toProtoBytes() {
    io.ray.serve.generated.DeploymentVersion.Builder proto =
        io.ray.serve.generated.DeploymentVersion.newBuilder();

    if (StringUtils.isNotBlank(codeVersion)) {
      proto.setCodeVersion(codeVersion);
    }
    proto.setDeploymentConfig(deploymentConfig.toProto());
    if (rayActorOptions != null && !rayActorOptions.isEmpty()) {
      proto.setRayActorOptions(gson.toJson(rayActorOptions));
    }
    return proto.build().toByteArray();
  }
}
