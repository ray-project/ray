package io.ray;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RuntimeEnv {
  private static ObjectMapper mapper = new ObjectMapper();

  public ObjectNode runtimeEnvs = mapper.createObjectNode();

  public void set(String name, Object typedRuntimeEnv) throws Exception {
    JsonNode node = mapper.valueToTree(typedRuntimeEnv);
    PluginSchemaManager.getInstance().validate(name, node);
    runtimeEnvs.set(name, node);
  }

  public <T> T get(String name, Class<T> classOfT) throws JsonProcessingException {
    return mapper.treeToValue(runtimeEnvs.get(name), classOfT);
  }

  public void remove(String name) {
    runtimeEnvs.remove(name);
  }

  public String serialize() throws JsonProcessingException {
    return mapper.writeValueAsString(runtimeEnvs);
  }

  public static RuntimeEnv deserialize(String serializedRuntimeEnv) throws JsonProcessingException {
    RuntimeEnv runtimeEnv = new RuntimeEnv();
    runtimeEnv.runtimeEnvs = (ObjectNode)mapper.readTree(serializedRuntimeEnv);
    return runtimeEnv;
  }
}
