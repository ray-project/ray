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
import io.ray.runtime.generated.RuntimeEnvCommon;
import java.io.IOException;

public class RuntimeEnvImpl implements RuntimeEnv {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public ObjectNode runtimeEnvs = MAPPER.createObjectNode();

  public RuntimeEnvImpl() {}

  @Override
  public void set(String name, Object value) throws RuntimeEnvException {
    JsonNode node = null;
    try {
      node = MAPPER.valueToTree(value);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    }
    runtimeEnvs.set(name, node);
  }

  @Override
  public void setJsonStr(String name, String jsonStr) throws RuntimeEnvException {
    JsonNode node = null;
    try {
      node = JsonLoader.fromString(jsonStr);
    } catch (IOException e) {
      throw new RuntimeException(e);
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
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getJsonStr(String name) throws RuntimeEnvException {
    try {
      return MAPPER.writeValueAsString(runtimeEnvs.get(name));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean remove(String name) {
    if (runtimeEnvs.has(name)) {
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
      throw new RuntimeException(e);
    }
  }

  @Override
  public String serializeToRuntimeEnvInfo() throws RuntimeEnvException {
    // TODO(SongGuyang): Expose runtime env config API to users.
    String serializeRuntimeEnv = serialize();
    RuntimeEnvCommon.RuntimeEnvInfo.Builder protoRuntimeEnvInfoBuilder =
        RuntimeEnvCommon.RuntimeEnvInfo.newBuilder();
    protoRuntimeEnvInfoBuilder.setSerializedRuntimeEnv(serializeRuntimeEnv);
    JsonFormat.Printer printer = JsonFormat.printer();
    try {
      return printer.print(protoRuntimeEnvInfoBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public RuntimeEnvCommon.RuntimeEnvInfo GenerateRuntimeEnvInfo() throws RuntimeEnvException {
    RuntimeEnvCommon.RuntimeEnvInfo.Builder protoRuntimeEnvInfoBuilder =
        RuntimeEnvCommon.RuntimeEnvInfo.newBuilder();

    try {
      protoRuntimeEnvInfoBuilder.setSerializedRuntimeEnv(MAPPER.writeValueAsString(runtimeEnvs));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return protoRuntimeEnvInfoBuilder.build();
  }
}
