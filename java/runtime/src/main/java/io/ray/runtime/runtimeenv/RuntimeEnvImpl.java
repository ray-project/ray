package io.ray.runtime.runtimeenv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.JsonLoader;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.ray.api.exception.RuntimeEnvException;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.api.runtimeenv.RuntimeEnvConfig;
import io.ray.runtime.generated.RuntimeEnvCommon;
import java.io.IOException;

public class RuntimeEnvImpl implements RuntimeEnv {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public ObjectNode runtimeEnvs = MAPPER.createObjectNode();

  private static final String CONFIG_FIELD_NAME = "config";

  public RuntimeEnvImpl() {}

  @Override
  public void set(String name, Object value) throws RuntimeEnvException {
    if (CONFIG_FIELD_NAME.equals(name) && value instanceof RuntimeEnvConfig == false) {
      throw new RuntimeEnvException(name + "must be instance of " + RuntimeEnvConfig.class);
    }
    JsonNode node = null;
    try {
      node = MAPPER.valueToTree(value);
    } catch (IllegalArgumentException e) {
      throw new RuntimeEnvException("Failed to set field.", e);
    }
    runtimeEnvs.set(name, node);
  }

  @Override
  public void setJsonStr(String name, String jsonStr) throws RuntimeEnvException {
    JsonNode node = null;
    try {
      node = JsonLoader.fromString(jsonStr);
    } catch (IOException e) {
      throw new RuntimeEnvException("Failed to set json field.", e);
    }
    runtimeEnvs.set(name, node);
  }

  @Override
  public <T> T get(String name, Class<T> classOfT) throws RuntimeEnvException {
    JsonNode jsonNode = runtimeEnvs.get(name);
    if (jsonNode == null) {
      return null;
    }
    try {
      return MAPPER.treeToValue(jsonNode, classOfT);
    } catch (JsonProcessingException e) {
      throw new RuntimeEnvException("Failed to get field.", e);
    }
  }

  @Override
  public String getJsonStr(String name) throws RuntimeEnvException {
    try {
      return MAPPER.writeValueAsString(runtimeEnvs.get(name));
    } catch (JsonProcessingException e) {
      throw new RuntimeEnvException("Failed to get json field.", e);
    }
  }

  @Override
  public boolean contains(String name) {
    return runtimeEnvs.has(name);
  }

  @Override
  public boolean remove(String name) {
    if (contains(name)) {
      runtimeEnvs.remove(name);
      return true;
    }
    return false;
  }

  @Override
  public String serialize() throws RuntimeEnvException {
    try {
      return MAPPER.writeValueAsString(runtimeEnvs);
    } catch (JsonProcessingException e) {
      throw new RuntimeEnvException("Failed to serialize.", e);
    }
  }

  @Override
  public boolean isEmpty() {
    return runtimeEnvs.isEmpty();
  }

  @Override
  public String serializeToRuntimeEnvInfo() throws RuntimeEnvException {
    RuntimeEnvCommon.RuntimeEnvInfo protoRuntimeEnvInfo = GenerateRuntimeEnvInfo();

    JsonFormat.Printer printer = JsonFormat.printer();
    try {
      return printer.print(protoRuntimeEnvInfo);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeEnvException("Failed to serialize to runtime env info.", e);
    }
  }

  @Override
  public void setConfig(RuntimeEnvConfig runtimeEnvConfig) {
    set(CONFIG_FIELD_NAME, runtimeEnvConfig);
  }

  @Override
  public RuntimeEnvConfig getConfig() {
    if (!contains(CONFIG_FIELD_NAME)) {
      return null;
    }
    return get(CONFIG_FIELD_NAME, RuntimeEnvConfig.class);
  }

  public RuntimeEnvCommon.RuntimeEnvInfo GenerateRuntimeEnvInfo() throws RuntimeEnvException {
    String serializeRuntimeEnv = serialize();
    RuntimeEnvCommon.RuntimeEnvInfo.Builder protoRuntimeEnvInfoBuilder =
        RuntimeEnvCommon.RuntimeEnvInfo.newBuilder();
    protoRuntimeEnvInfoBuilder.setSerializedRuntimeEnv(serializeRuntimeEnv);
    RuntimeEnvConfig runtimeEnvConfig = getConfig();
    if (runtimeEnvConfig != null) {
      RuntimeEnvCommon.RuntimeEnvConfig.Builder protoRuntimeEnvConfigBuilder =
          RuntimeEnvCommon.RuntimeEnvConfig.newBuilder();
      protoRuntimeEnvConfigBuilder.setSetupTimeoutSeconds(
          runtimeEnvConfig.getSetupTimeoutSeconds());
      protoRuntimeEnvConfigBuilder.setEagerInstall(runtimeEnvConfig.getEagerInstall());
      protoRuntimeEnvInfoBuilder.setRuntimeEnvConfig(protoRuntimeEnvConfigBuilder.build());
    }

    return protoRuntimeEnvInfoBuilder.build();
  }
}
