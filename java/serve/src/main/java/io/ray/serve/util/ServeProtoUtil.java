package io.ray.serve.util;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.Constants;
import io.ray.serve.DeploymentConfig;
import io.ray.serve.DeploymentVersion;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  public static DeploymentConfig parseDeploymentConfig(byte[] deploymentConfigBytes) {

    DeploymentConfig deploymentConfig = new DeploymentConfig();
    if (deploymentConfigBytes == null) {
      return deploymentConfig;
    }

    io.ray.serve.generated.DeploymentConfig pbDeploymentConfig = null;
    try {
      pbDeploymentConfig = io.ray.serve.generated.DeploymentConfig.parseFrom(deploymentConfigBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse DeploymentConfig from protobuf bytes.", e);
    }

    if (pbDeploymentConfig == null) {
      return deploymentConfig;
    }

    if (pbDeploymentConfig.getNumReplicas() != 0) {
      deploymentConfig.setNumReplicas(pbDeploymentConfig.getNumReplicas());
    }
    if (pbDeploymentConfig.getMaxConcurrentQueries() != 0) {
      deploymentConfig.setMaxConcurrentQueries(pbDeploymentConfig.getMaxConcurrentQueries());
    }
    if (pbDeploymentConfig.getGracefulShutdownWaitLoopS() != 0) {
      deploymentConfig.setGracefulShutdownWaitLoopS(
          pbDeploymentConfig.getGracefulShutdownWaitLoopS());
    }
    if (pbDeploymentConfig.getGracefulShutdownTimeoutS() != 0) {
      deploymentConfig.setGracefulShutdownTimeoutS(
          pbDeploymentConfig.getGracefulShutdownTimeoutS());
    }
    deploymentConfig.setCrossLanguage(pbDeploymentConfig.getIsCrossLanguage());
    if (pbDeploymentConfig.getDeploymentLanguage() == DeploymentLanguage.UNRECOGNIZED) {
      throw new RayServeException(
          LogUtil.format(
              "Unrecognized deployment language {}. Deployment language must be in {}.",
              pbDeploymentConfig.getDeploymentLanguage(),
              Lists.newArrayList(DeploymentLanguage.values())));
    }
    deploymentConfig.setDeploymentLanguage(pbDeploymentConfig.getDeploymentLanguageValue());
    if (pbDeploymentConfig.getUserConfig() != null
        && pbDeploymentConfig.getUserConfig().size() != 0) {
      deploymentConfig.setUserConfig(
          MessagePackSerializer.decode(
              pbDeploymentConfig.getUserConfig().toByteArray(), Object.class));
    }
    return deploymentConfig;
  }

  public static RequestMetadata parseRequestMetadata(byte[] requestMetadataBytes) {

    // Get a builder from RequestMetadata(bytes) or create a new one.
    RequestMetadata.Builder builder = null;
    if (requestMetadataBytes == null) {
      builder = RequestMetadata.newBuilder();
    } else {
      RequestMetadata requestMetadata = null;
      try {
        requestMetadata = RequestMetadata.parseFrom(requestMetadataBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse RequestMetadata from protobuf bytes.", e);
      }
      if (requestMetadata == null) {
        builder = RequestMetadata.newBuilder();
      } else {
        builder = RequestMetadata.newBuilder(requestMetadata);
      }
    }

    // Set default values.
    if (StringUtils.isBlank(builder.getCallMethod())) {
      builder.setCallMethod(Constants.CALL_METHOD);
    }

    return builder.build();
  }

  public static RequestWrapper parseRequestWrapper(byte[] httpRequestWrapperBytes) {

    // Get a builder from HTTPRequestWrapper(bytes) or create a new one.
    RequestWrapper.Builder builder = null;
    if (httpRequestWrapperBytes == null) {
      builder = RequestWrapper.newBuilder();
    } else {
      RequestWrapper requestWrapper = null;
      try {
        requestWrapper = RequestWrapper.parseFrom(httpRequestWrapperBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse RequestWrapper from protobuf bytes.", e);
      }
      if (requestWrapper == null) {
        builder = RequestWrapper.newBuilder();
      } else {
        builder = RequestWrapper.newBuilder(requestWrapper);
      }
    }

    return builder.build();
  }

  public static Map<String, EndpointInfo> parseEndpointSet(byte[] endpointSetBytes) {
    if (endpointSetBytes == null) {
      return null;
    }
    EndpointSet endpointSet = null;
    try {
      endpointSet = EndpointSet.parseFrom(endpointSetBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse EndpointSet from protobuf bytes.", e);
    }
    if (endpointSet == null) {
      return null;
    }
    return endpointSet.getEndpointsMap();
  }

  public static DeploymentVersion parseDeploymentVersion(byte[] deploymentVersionBytes) {
    if (deploymentVersionBytes == null) {
      return new DeploymentVersion();
    }

    io.ray.serve.generated.DeploymentVersion pbDeploymentVersion = null;
    try {
      pbDeploymentVersion =
          io.ray.serve.generated.DeploymentVersion.parseFrom(deploymentVersionBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse DeploymentVersion from protobuf bytes.", e);
    }
    if (pbDeploymentVersion == null) {
      return new DeploymentVersion();
    }
    return new DeploymentVersion(
        pbDeploymentVersion.getCodeVersion(),
        pbDeploymentVersion.getUserConfig() != null
                && pbDeploymentVersion.getUserConfig().size() != 0
            ? new Object[] {
              MessagePackSerializer.decode(
                  pbDeploymentVersion.getUserConfig().toByteArray(), Object.class)
            }
            : null);
  }

  public static io.ray.serve.generated.DeploymentVersion toProtobuf(
      DeploymentVersion deploymentVersion) {
    io.ray.serve.generated.DeploymentVersion.Builder pbDeploymentVersion =
        io.ray.serve.generated.DeploymentVersion.newBuilder();
    if (deploymentVersion == null) {
      return pbDeploymentVersion.build();
    }

    if (StringUtils.isNotBlank(deploymentVersion.getCodeVersion())) {
      pbDeploymentVersion.setCodeVersion(deploymentVersion.getCodeVersion());
    }
    if (deploymentVersion.getUserConfig() != null) {
      pbDeploymentVersion.setUserConfig(
          ByteString.copyFrom(
              MessagePackSerializer.encode(deploymentVersion.getUserConfig()).getLeft()));
    }
    return pbDeploymentVersion.build();
  }
}
