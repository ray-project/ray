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

  @Override
  public String toJsonBytes() {
    if (!envVars.isEmpty()) {
      RuntimeEnvCommon.RuntimeEnv.Builder protoRuntimeEnvBuilder =
          RuntimeEnvCommon.RuntimeEnv.newBuilder();
      protoRuntimeEnvBuilder.putAllEnvVars(envVars);
      JsonFormat.Printer printer = JsonFormat.printer();
      try {
        return printer.print(protoRuntimeEnvBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
    return "{}";
  }
}
