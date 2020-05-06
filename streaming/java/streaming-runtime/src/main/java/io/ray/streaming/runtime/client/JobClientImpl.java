package io.ray.streaming.runtime.client;

import java.util.HashMap;
import java.util.Map;
import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.options.ActorCreationOptions;
import io.ray.streaming.client.JobClient;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.config.global.CommonConfig;
import io.ray.streaming.runtime.master.JobMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job client: to submit job from api to runtime.
 */
public class JobClientImpl implements JobClient {

  public static final Logger LOG = LoggerFactory.getLogger(JobClientImpl.class);

  private RayActor<JobMaster> jobMasterActor;

  @Override
  public void submit(JobGraph jobGraph, Map<String, String> jobConfig) {
    LOG.info("Submit job [{}] with job graph [{}] and job config [{}].",
        jobGraph.getJobName(), jobGraph, jobConfig);
    Map<String, Double> resources = new HashMap<>();
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(resources)
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTION)
        .createActorCreationOptions();

    // set job name and id at start
    jobConfig.put(CommonConfig.JOB_ID, Ray.getRuntimeContext().getCurrentJobId().toString());
    jobConfig.put(CommonConfig.JOB_NAME, jobGraph.getJobName());

    this.jobMasterActor = Ray.createActor(JobMaster::new, jobConfig, options);
    RayObject<Boolean> submitResult = jobMasterActor.call(JobMaster::submitJob,
        jobMasterActor, jobGraph);

    if (submitResult.get()) {
      LOG.info("Submit job [{}] success.", jobGraph.getJobName());
    }
  }
}
