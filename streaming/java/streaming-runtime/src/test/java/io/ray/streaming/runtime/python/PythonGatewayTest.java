package io.ray.streaming.runtime.python;

import static org.testng.Assert.assertEquals;

import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobGraphBuilder;
import io.ray.streaming.runtime.serialization.MsgPackSerializer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class PythonGatewayTest {

  @Test
  public void testPythonGateway() {
    MsgPackSerializer serializer = new MsgPackSerializer();
    PythonGateway gateway = new PythonGateway();
    gateway.createStreamingContext();
    Map<String, String> config = new HashMap<>();
    config.put("k1", "v1");
    gateway.withConfig(serializer.serialize(config));
    byte[] mockPySource = new byte[0];
    Object source = serializer.deserialize(gateway.createPythonStreamSource(mockPySource));
    byte[] mockPyFunc = new byte[0];
    Object mapPyFunc = serializer.deserialize(gateway.createPyFunc(mockPyFunc));
    Object mapStream =
        serializer.deserialize(
            gateway.callMethod(serializer.serialize(Arrays.asList(source, "map", mapPyFunc))));
    byte[] mockPyPartition = new byte[0];
    Object partition = serializer.deserialize(gateway.createPyPartition(mockPyPartition));
    Object partitionedStream =
        serializer.deserialize(
            gateway.callMethod(
                serializer.serialize(Arrays.asList(mapStream, "partitionBy", partition))));
    byte[] mockSinkFunc = new byte[0];
    Object sinkPyFunc = serializer.deserialize(gateway.createPyFunc(mockSinkFunc));
    gateway.callMethod(serializer.serialize(Arrays.asList(partitionedStream, "sink", sinkPyFunc)));
    List<StreamSink> streamSinks = gateway.getStreamingContext().getStreamSinks();
    assertEquals(streamSinks.size(), 1);
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(streamSinks, "py_job");
    JobGraph jobGraph = jobGraphBuilder.build();
    jobGraph.printJobGraph();
  }
}
