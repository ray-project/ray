package org.ray.streaming.jobgraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.Stream;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.stream.PythonDataStream;

public class JobGraphBuilder {

  private JobGraph jobGraph;

  private AtomicInteger edgeIdGenerator;
  private List<StreamSink> streamSinkList;

  public JobGraphBuilder(List<StreamSink> streamSinkList) {
    this(streamSinkList, "job-" + System.currentTimeMillis());
  }

  public JobGraphBuilder(List<StreamSink> streamSinkList, String jobName) {
    this(streamSinkList, jobName, new HashMap<>());
  }

  public JobGraphBuilder(List<StreamSink> streamSinkList, String jobName,
                         Map<String, String> jobConfig) {
    this.jobGraph = new JobGraph(jobName, jobConfig);
    this.streamSinkList = streamSinkList;
    this.edgeIdGenerator = new AtomicInteger(0);
  }

  public JobGraph build() {
    for (StreamSink streamSink : streamSinkList) {
      processStream(streamSink);
    }
    return this.jobGraph;
  }

  private void processStream(Stream stream) {
    int vertexId = stream.getId();
    int parallelism = stream.getParallelism();

    StreamOperator streamOperator = stream.getOperator();
    JobVertex jobVertex = null;

    if (stream instanceof StreamSink) {
      jobVertex = new JobVertex(vertexId, parallelism, VertexType.SINK, streamOperator);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      JobEdge jobEdge = new JobEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.jobGraph.addEdge(jobEdge);
      processStream(parentStream);
    } else if (stream instanceof StreamSource) {
      jobVertex = new JobVertex(vertexId, parallelism, VertexType.SOURCE, streamOperator);
    } else if (stream instanceof DataStream || stream instanceof PythonDataStream) {
      jobVertex = new JobVertex(vertexId, parallelism, VertexType.TRANSFORMATION, streamOperator);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      JobEdge jobEdge = new JobEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.jobGraph.addEdge(jobEdge);
      processStream(parentStream);
    } else {
      throw new UnsupportedOperationException("Unsupported stream: " + stream);
    }
    this.jobGraph.addVertex(jobVertex);
  }

  private int getEdgeId() {
    return this.edgeIdGenerator.incrementAndGet();
  }

}
