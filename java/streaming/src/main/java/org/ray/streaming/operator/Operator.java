package org.ray.streaming.operator;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.core.runtime.context.RuntimeContext;
import java.io.Serializable;
import java.util.List;

public interface Operator extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void finish();

  void close();

  OperatorType getOpType();
}
