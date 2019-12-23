package org.ray.streaming.python.descriptor;

import org.ray.streaming.operator.OperatorType;
import org.ray.streaming.operator.StreamOperator;

public class DescriptorOperator extends StreamOperator implements Descriptor {
  private String moduleName;
  private String className;

  private DescriptorOperator(String moduleName,
                             String className,
                             DescriptorFunction function) {
    super(function);
    this.moduleName = moduleName;
    this.className = className;
  }

  @Override
  public OperatorType getOpType() {
    throw new RuntimeException("DescriptorOperator methods shouldn't be called in java");
  }

  @Override
  public byte[] toBytes() {
    // TODO serialize to bytes using protobuf
    return new byte[0];
  }

  public static DescriptorOperator ofSource(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "SourceOperator", function);
  }

  public static DescriptorOperator ofMap(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "MapOperator", function);
  }

  public static DescriptorOperator ofFlatMap(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "FlatMapOperator", function);
  }

  public static DescriptorOperator ofFilter(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "FilterOperator", function);
  }

  public static DescriptorOperator ofKeyBy(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "KeyByOperator", function);
  }

  public static DescriptorOperator ofReduce(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "ReduceOperator", function);
  }

  public static DescriptorOperator ofSink(DescriptorFunction function) {
    return new DescriptorOperator(
        "ray.streaming.operator",
        "SinkOperator", function);
  }

}
