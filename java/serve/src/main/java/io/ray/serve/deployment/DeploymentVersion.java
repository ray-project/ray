package io.ray.serve.deployment;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.exception.RayServeException;
import java.io.Serializable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

public class DeploymentVersion implements Serializable {

  private static final long serialVersionUID = 3400261981775851058L;

  private String codeVersion;

  private Object userConfig;

  private boolean unversioned;

  public DeploymentVersion() {
    this(null, null);
  }

  public DeploymentVersion(String codeVersion) {
    this(codeVersion, null);
  }

  public DeploymentVersion(String codeVersion, Object userConfig) {
    if (StringUtils.isBlank(codeVersion)) {
      this.unversioned = true;
      this.codeVersion = RandomStringUtils.randomAlphabetic(6);
    } else {
      this.codeVersion = codeVersion;
    }
    this.userConfig = userConfig;
  }

  public String getCodeVersion() {
    return codeVersion;
  }

  public Object getUserConfig() {
    return userConfig;
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
        proto.getUserConfig() != null && proto.getUserConfig().size() != 0
            ? new Object[] {
              MessagePackSerializer.decode(
                  proto.getUserConfig().toByteArray(), Object.class) // TODO-xlang
            }
            : null);
  }

  public byte[] toProtoBytes() {
    io.ray.serve.generated.DeploymentVersion.Builder proto =
        io.ray.serve.generated.DeploymentVersion.newBuilder();

    if (StringUtils.isNotBlank(codeVersion)) {
      proto.setCodeVersion(codeVersion);
    }
    if (userConfig != null) {
      proto.setUserConfig(
          ByteString.copyFrom(MessagePackSerializer.encode(userConfig).getLeft())); // TODO-xlang
    }
    return proto.build().toByteArray();
  }
}
