package io.ray.streaming.python.stream;

import io.ray.streaming.python.PythonOperator;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a union DataStream.
 *
 * <p>This stream does not create a physical operation, it only affects how upstream data are
 *  connected to downstream data.
 */
public class PythonUnionStream extends PythonDataStream {
  private List<PythonDataStream> unionStreams;

  public PythonUnionStream(PythonDataStream input, List<PythonDataStream> others) {
    // Union stream does not create a physical operation, so we don't have to set partition
    // function for it.
    super(input, new PythonOperator(
        "ray.streaming.operator", "UnionOperator"));
    this.unionStreams = new ArrayList<>();
    others.forEach(this::addStream);
  }

  void addStream(PythonDataStream stream) {
    if (stream instanceof PythonUnionStream) {
      this.unionStreams.addAll(((PythonUnionStream) stream).getUnionStreams());
    } else {
      this.unionStreams.add(stream);
    }
  }

  public List<PythonDataStream> getUnionStreams() {
    return unionStreams;
  }
}