package io.ray.streaming.python;

import io.ray.streaming.api.partition.Partition;

/**
 * Represents a python partition function.
 * <p>
 * Python worker can create a partition object using information in this
 * PythonPartition.
 * <p>
 * If this object is constructed from serialized python partition,
 * python worker can deserialize it to create python partition directly.
 * If this object is constructed from moduleName and className/functionName,
 * python worker will use `importlib` to load python partition function.
 * <p>
 * TODO serialize to bytes using protobuf
 */
public class PythonPartition implements Partition {
  public static final PythonPartition BroadcastPartition = new PythonPartition(
      "ray.streaming.partition", "BroadcastPartition", null);
  public static final PythonPartition KeyPartition = new PythonPartition(
      "ray.streaming.partition", "KeyPartition", null);
  public static final PythonPartition RoundRobinPartition = new PythonPartition(
      "ray.streaming.partition", "RoundRobinPartition", null);

  private byte[] partition;
  private String moduleName;
  private String className;
  private String functionName;

  public PythonPartition(byte[] partition) {
    this.partition = partition;
  }

  public PythonPartition(String moduleName, String className, String functionName) {
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  @Override
  public int[] partition(Object record, int numPartition) {
    String msg = String.format("partition method of %s shouldn't be called.",
        getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  public byte[] getPartition() {
    return partition;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getClassName() {
    return className;
  }

  public String getFunctionName() {
    return functionName;
  }
}
