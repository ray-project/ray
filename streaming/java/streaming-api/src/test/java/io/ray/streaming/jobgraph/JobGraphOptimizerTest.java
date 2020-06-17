package io.ray.streaming.jobgraph;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.python.PythonFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class JobGraphOptimizerTest {
  private static final Logger LOG = LoggerFactory.getLogger( JobGraphOptimizerTest.class );

  @Test
  public void testOptimize() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<Integer> source1 = DataStreamSource.fromCollection(context,
        Lists.newArrayList(1 ,2 ,3));
    DataStream<String> source2 = DataStreamSource.fromCollection(context,
        Lists.newArrayList("1", "2", "3"));
    DataStream<String> source3 = DataStreamSource.fromCollection(context,
        Lists.newArrayList("2", "3", "4"));
    source1.filter(x -> x > 1)
        .map(String::valueOf)
        .union(source2)
        .join(source3)
        .sink(x -> System.out.println("Sink " + x));
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 8);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 5);
  }

  @Test
  public void testOptimizeHybridStream() {
    StreamingContext context = StreamingContext.buildContext();
    DataStream<Integer> source1 = DataStreamSource.fromCollection(context,
        Lists.newArrayList(1 ,2 ,3));
    DataStream<String> source2 = DataStreamSource.fromCollection(context,
        Lists.newArrayList("1", "2", "3"));
    source1.asPythonStream()
        .map(pyFunc(1))
        .filter(pyFunc(2))
        .union(source2.asPythonStream().filter(pyFunc(3)).map(pyFunc(4)))
        .asJavaStream()
        .sink(x -> System.out.println("Sink " + x));
    JobGraph jobGraph = new JobGraphBuilder(context.getStreamSinks()).build();
    LOG.info("Digraph {}", jobGraph.generateDigraph());
    assertEquals(jobGraph.getJobVertices().size(), 8);

    JobGraphOptimizer graphOptimizer = new JobGraphOptimizer(jobGraph);
    JobGraph optimizedJobGraph = graphOptimizer.optimize();
    optimizedJobGraph.printJobGraph();
    LOG.info("Optimized graph {}", optimizedJobGraph.generateDigraph());
    assertEquals(optimizedJobGraph.getJobVertices().size(), 6);
  }

  private PythonFunction pyFunc(int number) {
    return new PythonFunction("module", "func" + number);
  }

}