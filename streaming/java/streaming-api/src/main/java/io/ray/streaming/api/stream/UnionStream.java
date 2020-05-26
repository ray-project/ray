package io.ray.streaming.api.stream;

import io.ray.streaming.operator.StreamOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a union DataStream.
 *
 * @param <T> The type of union data.
 */
public class UnionStream<T> extends DataStream<T> {
  private List<DataStream<T>> unionStreams;

  @SafeVarargs
  public UnionStream(DataStream<T> input, StreamOperator streamOperator, DataStream<T>... others) {
    super(input, streamOperator);
    this.unionStreams = new ArrayList<>();
    Arrays.stream(others).forEach(this::addStream);
  }

  void addStream(DataStream<T> stream) {
    if (stream instanceof UnionStream) {
      this.unionStreams.addAll(((UnionStream<T>) stream).getUnionStreams());
    } else {
      this.unionStreams.add(stream);
    }
  }

  public List<DataStream<T>> getUnionStreams() {
    return unionStreams;
  }
}
