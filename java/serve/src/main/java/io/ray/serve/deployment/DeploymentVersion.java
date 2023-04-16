package io.ray.serve.deployment;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;
import io.ray.serve.exception.RayServeException;
import java.io.Serializable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

public class DeploymentVersion implements Serializable {

  private static final long serialVersionUID = 3400261981775851058L;

  private String codeVersion;

  private Object userConfig;

  private DeploymentConfig deploymentConfig;

  private ReplicaConfig replicaConfig;

  private boolean unversioned;

  public DeploymentVersion() {
    this(null, new DeploymentConfig(), null);
  }

  public DeploymentVersion(String codeVersion) {
    this(codeVersion, new DeploymentConfig(), null);
  }

  public DeploymentVersion(
      String codeVersion, DeploymentConfig deploymentConfig, ReplicaConfig replicaConfig) {
    if (StringUtils.isBlank(codeVersion)) {
      this.unversioned = true;
      this.codeVersion = RandomStringUtils.randomAlphabetic(6);
    } else {
      this.codeVersion = codeVersion;
    }
    this.deploymentConfig = deploymentConfig;
    this.replicaConfig = replicaConfig;
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

  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
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
        ReplicaConfig.fromProto(proto.getReplicaConfig()));
  }

  public byte[] toProtoBytes() {
    io.ray.serve.generated.DeploymentVersion.Builder proto =
        io.ray.serve.generated.DeploymentVersion.newBuilder();

    if (StringUtils.isNotBlank(codeVersion)) {
      proto.setCodeVersion(codeVersion);
    }
    proto.setDeploymentConfig(deploymentConfig.toProto());
    proto.setReplicaConfig(replicaConfig.toProto());
    return proto.build().toByteArray();
  }
}
