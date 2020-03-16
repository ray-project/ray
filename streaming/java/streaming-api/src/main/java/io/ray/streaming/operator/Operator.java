package io.ray.streaming.operator;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import java.io.Serializable;
import java.util.List;
<<<<<<< HEAD:streaming/java/streaming-api/src/main/java/io/ray/streaming/operator/Operator.java
=======
import org.ray.streaming.api.Language;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.StreamRuntimeContext;
import org.ray.streaming.api.function.Function;
>>>>>>> add state module:streaming/java/streaming-api/src/main/java/org/ray/streaming/operator/Operator.java

public interface Operator extends Serializable {

  void open(List<Collector> collectors, StreamRuntimeContext runtimeContext);

  void finish();

  void close();

  Function getFunction();

  Language getLanguage();

  OperatorType getOpType();
}
