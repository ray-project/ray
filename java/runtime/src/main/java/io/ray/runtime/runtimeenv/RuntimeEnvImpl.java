package io.ray.runtime.runtimeenv;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.generated.RuntimeEnvCommon;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuntimeEnvImpl implements RuntimeEnv {

  private Map<String, String> envVars = new HashMap<>();

  private List<String> jars = new ArrayList<>();

  public RuntimeEnvImpl(Map<String, String> envVars, List<String> jars) {
    this.envVars = envVars;
    if (jars != null) {
      this.jars = jars;
    }
  }

  public Map<String, String> getEnvVars() {
    return envVars;
  }

  @Override
  public String toJsonBytes() {
    // Get serializedRuntimeEnv
    String serializedRuntimeEnv = "{}";

    RuntimeEnvCommon.RuntimeEnv.Builder protoRuntimeEnvBuilder =
        RuntimeEnvCommon.RuntimeEnv.newBuilder();
    JsonFormat.Printer printer = JsonFormat.printer();
    if (!envVars.isEmpty()) {
      protoRuntimeEnvBuilder.putAllEnvVars(envVars);
    }
    if (!jars.isEmpty()) {
      protoRuntimeEnvBuilder.getJavaRuntimeEnvBuilder().addAllDependentJars(jars);
    }

    try {
      serializedRuntimeEnv = printer.print(protoRuntimeEnvBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    // Get serializedRuntimeEnvInfo
    if (serializedRuntimeEnv.equals("{}") || serializedRuntimeEnv.isEmpty()) {
      return "{}";
    }
    RuntimeEnvCommon.RuntimeEnvInfo.Builder protoRuntimeEnvInfoBuilder =
        RuntimeEnvCommon.RuntimeEnvInfo.newBuilder();
    protoRuntimeEnvInfoBuilder.setSerializedRuntimeEnv(serializedRuntimeEnv);
    printer = JsonFormat.printer();
    try {
      return printer.print(protoRuntimeEnvInfoBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
