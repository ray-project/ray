package io.ray.streaming.python.stream;

import io.ray.streaming.python.PythonOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PythonUnionStream extends PythonDataStream {
  private List<PythonDataStream> unionStreams;

  public PythonUnionStream(PythonDataStream input, PythonDataStream... others) {
    super(input, new PythonOperator(
        "ray.streaming.operator", "UnionOperator"));
    this.unionStreams = new ArrayList<>();
    Arrays.stream(others).forEach(this::addStream);
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