package io.ray.streaming.jobgraph;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.JoinStream;
import io.ray.streaming.api.stream.Stream;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.api.stream.StreamSource;
import io.ray.streaming.api.stream.UnionStream;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonUnionStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobGraphBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(JobGraphBuilder.class);

  private JobGraph jobGraph;

  private AtomicInteger edgeIdGenerator;
  private List<StreamSink> streamSinkList;

  public JobGraphBuilder(List<StreamSink> streamSinkList) {
    this(streamSinkList, "job_" + System.currentTimeMillis());
  }

  public JobGraphBuilder(List<StreamSink> streamSinkList, String jobName) {
    this(streamSinkList, jobName, new HashMap<>());
  }

  public JobGraphBuilder(
      List<StreamSink> streamSinkList, String jobName, Map<String, String> jobConfig) {
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

  @SuppressWarnings("unchecked")
  private void processStream(Stream stream) {
    while (stream.isProxyStream()) {
      // Proxy stream and original stream are the same logical stream, both refer to the
      // same data flow transformation. We should skip proxy stream to avoid applying same
      // transformation multiple times.
      LOG.debug("Skip proxy stream {} of id {}", stream, stream.getId());
      stream = stream.getOriginalStream();
    }
    StreamOperator streamOperator = stream.getOperator();
    Preconditions.checkArgument(
        stream.getLanguage() == streamOperator.getLanguage(),
        "Reference stream should be skipped.");
    int vertexId = stream.getId();
    int parallelism = stream.getParallelism();
    Map<String, String> config = stream.getConfig();
    JobVertex jobVertex;
    if (stream instanceof StreamSink) {
      jobVertex = new JobVertex(vertexId, parallelism, VertexType.SINK, streamOperator, config);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      JobEdge jobEdge = new JobEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.jobGraph.addEdge(jobEdge);
      processStream(parentStream);
    } else if (stream instanceof StreamSource) {
      jobVertex = new JobVertex(vertexId, parallelism, VertexType.SOURCE, streamOperator, config);
    } else if (stream instanceof DataStream || stream instanceof PythonDataStream) {
      jobVertex =
          new JobVertex(vertexId, parallelism, VertexType.TRANSFORMATION, streamOperator, config);
      Stream parentStream = stream.getInputStream();
      int inputVertexId = parentStream.getId();
      JobEdge jobEdge = new JobEdge(inputVertexId, vertexId, parentStream.getPartition());
      this.jobGraph.addEdge(jobEdge);
      processStream(parentStream);

      // process union stream
      List<Stream> streams = new ArrayList<>();
      if (stream instanceof UnionStream) {
        streams.addAll(((UnionStream) stream).getUnionStreams());
      }
      if (stream instanceof PythonUnionStream) {
        streams.addAll(((PythonUnionStream) stream).getUnionStreams());
      }
      for (Stream otherStream : streams) {
        JobEdge otherEdge = new JobEdge(otherStream.getId(), vertexId, otherStream.getPartition());
        this.jobGraph.addEdge(otherEdge);
        processStream(otherStream);
      }

      // process join stream
      if (stream instanceof JoinStream) {
        DataStream rightStream = ((JoinStream) stream).getRightStream();
        this.jobGraph.addEdge(
            new JobEdge(rightStream.getId(), vertexId, rightStream.getPartition()));
        processStream(rightStream);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported stream: " + stream);
    }
    this.jobGraph.addVertex(jobVertex);
  }

  private int getEdgeId() {
    return this.edgeIdGenerator.incrementAndGet();
  }
}
