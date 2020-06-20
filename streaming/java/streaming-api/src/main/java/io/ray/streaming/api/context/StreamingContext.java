package io.ray.streaming.api.context;

import com.google.common.base.Preconditions;
import io.ray.api.Ray;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.client.JobClient;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobGraphBuilder;
import io.ray.streaming.jobgraph.JobGraphOptimizer;
import io.ray.streaming.util.Config;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulate the context information of a streaming Job.
 */
public class StreamingContext implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingContext.class);

  private transient AtomicInteger idGenerator;

  /**
   * The sinks of this streaming job.
   */
  private List<StreamSink> streamSinks;

  /**
   * The user custom streaming job configuration.
   */
  private Map<String, String> jobConfig;

  /**
   * The logic plan.
   */
  private JobGraph jobGraph;

  private StreamingContext() {
    this.idGenerator = new AtomicInteger(0);
    this.streamSinks = new ArrayList<>();
    this.jobConfig = new HashMap<>();
  }

  public static StreamingContext buildContext() {
    return new StreamingContext();
  }

  /**
   * Construct job DAG, and execute the job.
   */
  public void execute(String jobName) {
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(this.streamSinks, jobName);
    JobGraph originalJobGraph = jobGraphBuilder.build();
    this.jobGraph = new JobGraphOptimizer(originalJobGraph).optimize();
    jobGraph.printJobGraph();
    LOG.info("JobGraph digraph\n{}", jobGraph.generateDigraph());

    if (Ray.internal() == null) {
      if (Config.MEMORY_CHANNEL.equalsIgnoreCase(jobConfig.get(Config.CHANNEL_TYPE))) {
        Preconditions.checkArgument(!jobGraph.isCrossLanguageGraph());
        ClusterStarter.startCluster(false, true);
        LOG.info("Created local cluster for job {}.", jobName);
      } else {
        ClusterStarter.startCluster(jobGraph.isCrossLanguageGraph(), false);
        LOG.info("Created multi process cluster for job {}.", jobName);
      }
      Runtime.getRuntime().addShutdownHook(new Thread(StreamingContext.this::stop));
    } else {
      LOG.info("Reuse existing cluster.");
    }

    ServiceLoader<JobClient> serviceLoader = ServiceLoader.load(JobClient.class);
    Iterator<JobClient> iterator = serviceLoader.iterator();
    Preconditions.checkArgument(iterator.hasNext(),
        "No JobClient implementation has been provided.");
    JobClient jobClient = iterator.next();
    jobClient.submit(jobGraph, jobConfig);
  }

  public int generateId() {
    return this.idGenerator.incrementAndGet();
  }

  public void addSink(StreamSink streamSink) {
    streamSinks.add(streamSink);
  }

  public List<StreamSink> getStreamSinks() {
    return streamSinks;
  }

  public void withConfig(Map<String, String> jobConfig) {
    this.jobConfig = jobConfig;
  }

  public void stop() {
    if (Ray.internal() != null) {
      ClusterStarter.stopCluster(jobGraph.isCrossLanguageGraph());
    }
  }
}
