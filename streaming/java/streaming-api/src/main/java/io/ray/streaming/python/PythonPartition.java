package io.ray.streaming.python;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.partition.Partition;
import java.util.StringJoiner;
import org.apache.commons.lang3.StringUtils;

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
 */
public class PythonPartition implements Partition<Object> {
  public static final PythonPartition BroadcastPartition = new PythonPartition(
      "ray.streaming.partition", "BroadcastPartition");
  public static final PythonPartition KeyPartition = new PythonPartition(
      "ray.streaming.partition", "KeyPartition");
  public static final PythonPartition RoundRobinPartition = new PythonPartition(
      "ray.streaming.partition", "RoundRobinPartition");
  public static final String FORWARD_PARTITION_CLASS = "ForwardPartition";
  public static final PythonPartition ForwardPartition = new PythonPartition(
      "ray.streaming.partition", FORWARD_PARTITION_CLASS);

  private byte[] partition;
  private String moduleName;
  private String functionName;

  public PythonPartition(byte[] partition) {
    Preconditions.checkNotNull(partition);
    this.partition = partition;
  }

  /**
   * Create a python partition from a moduleName and partition function name
   *
   * @param moduleName module name of python partition
   * @param functionName function/class name of the partition function.
   */
  public PythonPartition(String moduleName, String functionName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(moduleName));
    Preconditions.checkArgument(StringUtils.isNotBlank(functionName));
    this.moduleName = moduleName;
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

  public String getFunctionName() {
    return functionName;
  }

  public boolean isConstructedFromBinary() {
    return partition != null;
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(", ",
        PythonPartition.class.getSimpleName() + "[", "]");
    if (partition != null) {
      stringJoiner.add("partition=binary partition");
    } else {
      stringJoiner.add("moduleName='" + moduleName + "'")
          .add("functionName='" + functionName + "'");
    }
    return stringJoiner.toString();
  }

}
