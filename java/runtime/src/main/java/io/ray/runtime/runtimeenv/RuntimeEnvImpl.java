package io.ray.runtime.runtimeenv;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.generated.RuntimeEnvCommon;
import java.util.HashMap;
import java.util.Map;

public class RuntimeEnvImpl implements RuntimeEnv {

  private Map<String, String> envVars = new HashMap<>();

  public RuntimeEnvImpl(Map<String, String> envVars) {
    this.envVars = envVars;
  }

  public Map<String, String> getEnvVars() {
    return envVars;
  }

  @Override
  public String toJsonBytes() {
    // Get serializedRuntimeEnv
    String serializedRuntimeEnv = "{}";
    if (!envVars.isEmpty()) {
      RuntimeEnvCommon.RuntimeEnv.Builder protoRuntimeEnvBuilder =
          RuntimeEnvCommon.RuntimeEnv.newBuilder();
      protoRuntimeEnvBuilder.putAllEnvVars(envVars);
      JsonFormat.Printer printer = JsonFormat.printer();
      try {
        serializedRuntimeEnv = printer.print(protoRuntimeEnvBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }

    // Get serializedRuntimeEnvInfo
    if (serializedRuntimeEnv.equals("{}") || serializedRuntimeEnv.isEmpty()) {
      return "{}";
    }
    RuntimeEnvCommon.RuntimeEnvInfo.Builder protoRuntimeEnvInfoBuilder =
        RuntimeEnvCommon.RuntimeEnvInfo.newBuilder();
    protoRuntimeEnvInfoBuilder.setSerializedRuntimeEnv(serializedRuntimeEnv);
    JsonFormat.Printer printer = JsonFormat.printer();
    try {
      return printer.print(protoRuntimeEnvInfoBuilder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
