package io.ray.runtime.runtimeenv;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.ray.api.runtimeenv.RuntimeEnvInfo;
import io.ray.runtime.generated.RuntimeEnvCommon;

public class RuntimeEnvInfoImpl implements RuntimeEnvInfo {

  private String serializedRuntimeEnv = "{}";

  public RuntimeEnvInfoImpl(String serializedRuntimeEnv) {
    this.serializedRuntimeEnv = serializedRuntimeEnv;
  }

  @Override
  public String toJsonBytes() {
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
