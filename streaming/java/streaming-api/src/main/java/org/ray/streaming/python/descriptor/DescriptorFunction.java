package org.ray.streaming.python.descriptor;

import org.ray.streaming.api.function.Function;

public class DescriptorFunction implements Descriptor, Function {
  private byte[] serializedPyFunction;
  private String moduleName;
  private String functionName;

  public DescriptorFunction(byte[] serializedPyFunction) {
    this.serializedPyFunction = serializedPyFunction;
  }

  public DescriptorFunction(String moduleName, String functionName) {
    this.moduleName = moduleName;
    this.functionName = functionName;
  }

  @Override
  public byte[] toBytes() {
    // TODO serialize to bytes using protobuf
    return new byte[0];
  }
}
