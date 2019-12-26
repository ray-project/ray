package org.ray.streaming.python.descriptor;

import org.ray.streaming.api.partition.Partition;

public class DescriptorPartition implements Descriptor, Partition {
  public static final DescriptorPartition BroadcastPartition = new DescriptorPartition(
      "ray.streaming.partition", "BroadcastPartition", null);
  public static final DescriptorPartition KeyPartition = new DescriptorPartition(
      "ray.streaming.partition", "KeyPartition", null);
  public static final DescriptorPartition RoundRobinPartition = new DescriptorPartition(
      "ray.streaming.partition", "RoundRobinPartition", null);

  private byte[] serializedPyPartition;
  private String moduleName;
  private String className;
  private String functionName;

  public DescriptorPartition(byte[] serializedPyPartition) {
    this.serializedPyPartition = serializedPyPartition;
  }

  public DescriptorPartition(String moduleName, String functionName) {
    this.moduleName = moduleName;
    this.functionName = functionName;
  }

  public DescriptorPartition(String moduleName, String className, String functionName) {
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  @Override
  public int[] partition(Object record, int numPartition) {
    throw new RuntimeException("DescriptorPartition methods shouldn't be called in java");
  }

  @Override
  public byte[] toBytes() {
    // TODO serialize to bytes using protobuf
    return new byte[0];
  }

}
