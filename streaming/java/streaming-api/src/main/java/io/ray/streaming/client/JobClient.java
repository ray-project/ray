package io.ray.streaming.client;

import io.ray.streaming.jobgraph.JobGraph;
import java.util.Map;

/**
 * Interface of the job client.
 */
public interface JobClient {

  /**
   * Submit job with logical plan to run.
   *
   * @param jobGraph The logical plan.
   */
  void submit(JobGraph jobGraph, Map<String, String> conf);
}
