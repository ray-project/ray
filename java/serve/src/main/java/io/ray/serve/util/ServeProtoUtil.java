package io.ray.serve.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.Constants;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.DeploymentVersion;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.generated.LongPollResult;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.generated.UpdatedObject;
import io.ray.serve.poll.KeyType;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  private static final Gson GSON = new Gson();

  public static DeploymentConfig parseDeploymentConfig(byte[] deploymentConfigBytes) {

    // Get a builder from DeploymentConfig(bytes) or create a new one.
    DeploymentConfig.Builder builder = null;
    if (deploymentConfigBytes == null) {
      builder = DeploymentConfig.newBuilder();
    } else {
      DeploymentConfig deploymentConfig = null;
      try {
        deploymentConfig = DeploymentConfig.parseFrom(deploymentConfigBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse DeploymentConfig from protobuf bytes.", e);
      }
      if (deploymentConfig == null) {
        builder = DeploymentConfig.newBuilder();
      } else {
        builder = DeploymentConfig.newBuilder(deploymentConfig);
      }
    }

    // Set default values.
    if (builder.getNumReplicas() == 0) {
      builder.setNumReplicas(1);
    }

    Preconditions.checkArgument(
        builder.getMaxConcurrentQueries() >= 0, "max_concurrent_queries must be >= 0");
    if (builder.getMaxConcurrentQueries() == 0) {
      builder.setMaxConcurrentQueries(100);
    }

    if (builder.getGracefulShutdownWaitLoopS() == 0) {
      builder.setGracefulShutdownWaitLoopS(2);
    }

    if (builder.getGracefulShutdownTimeoutS() == 0) {
      builder.setGracefulShutdownTimeoutS(20);
    }

    if (builder.getDeploymentLanguage() == DeploymentLanguage.UNRECOGNIZED) {
      throw new RayServeException(
          LogUtil.format(
              "Unrecognized backend language {}. Backend language must be in {}.",
              builder.getDeploymentLanguageValue(),
              Lists.newArrayList(DeploymentLanguage.values())));
    }

    return builder.build();
  }

  public static Object parseUserConfig(DeploymentConfig deploymentConfig) {
    if (deploymentConfig.getUserConfig() == null || deploymentConfig.getUserConfig().size() == 0) {
      return null;
    }
    return MessagePackSerializer.decode(
        deploymentConfig.getUserConfig().toByteArray(), Object.class);
  }

  public static RequestMetadata parseRequestMetadata(byte[] requestMetadataBytes)
      throws InvalidProtocolBufferException {

    // Get a builder from RequestMetadata(bytes) or create a new one.
    RequestMetadata.Builder builder = null;
    if (requestMetadataBytes == null) {
      builder = RequestMetadata.newBuilder();
    } else {
      RequestMetadata requestMetadata = RequestMetadata.parseFrom(requestMetadataBytes);
      if (requestMetadata == null) {
        builder = RequestMetadata.newBuilder();
      } else {
        builder = RequestMetadata.newBuilder(requestMetadata);
      }
    }

    // Set default values.
    if (StringUtils.isBlank(builder.getCallMethod())) {
      builder.setCallMethod(Constants.DEFAULT_CALL_METHOD);
    }

    return builder.build();
  }

  public static RequestWrapper parseRequestWrapper(byte[] httpRequestWrapperBytes)
      throws InvalidProtocolBufferException {

    // Get a builder from HTTPRequestWrapper(bytes) or create a new one.
    RequestWrapper.Builder builder = null;
    if (httpRequestWrapperBytes == null) {
      builder = RequestWrapper.newBuilder();
    } else {
      RequestWrapper requestWrapper = RequestWrapper.parseFrom(httpRequestWrapperBytes);
      if (requestWrapper == null) {
        builder = RequestWrapper.newBuilder();
      } else {
        builder = RequestWrapper.newBuilder(requestWrapper);
      }
    }

    return builder.build();
  }

  public static Map<KeyType, UpdatedObject> parseUpdatedObjects(byte[] longPollResultBytes)
      throws InvalidProtocolBufferException {
    if (longPollResultBytes == null) {
      return null;
    }
    LongPollResult longPollResult = LongPollResult.parseFrom(longPollResultBytes);
    Map<String, UpdatedObject> updatedObjects = longPollResult.getUpdatedObjectsMap();
    if (updatedObjects == null || updatedObjects.isEmpty()) {
      return null;
    }
    Map<KeyType, UpdatedObject> udpates = new HashMap<>(updatedObjects.size());
    updatedObjects.forEach(
        (key, value) -> udpates.put(ServeProtoUtil.GSON.fromJson(key, KeyType.class), value));
    return udpates;
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
      return null;
    }
    try {
      return DeploymentVersion.parseFrom(deploymentVersionBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse DeploymentVersion from protobuf bytes.", e);
    }
  }
}
