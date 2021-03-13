package io.ray.streaming.runtime.client;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.streaming.client.JobClient;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.config.global.CommonConfig;
import io.ray.streaming.runtime.master.JobMaster;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Job client: to submit job from api to runtime. */
public class JobClientImpl implements JobClient {

  public static final Logger LOG = LoggerFactory.getLogger(JobClientImpl.class);

  private ActorHandle<JobMaster> jobMasterActor;

  @Override
  public void submit(JobGraph jobGraph, Map<String, String> jobConfig) {
    LOG.info(
        "Submitting job [{}] with job graph [{}] and job config [{}].",
        jobGraph.getJobName(),
        jobGraph,
        jobConfig);
    Map<String, Double> resources = new HashMap<>();

    // set job name and id at start
    jobConfig.put(CommonConfig.JOB_ID, Ray.getRuntimeContext().getCurrentJobId().toString());
    jobConfig.put(CommonConfig.JOB_NAME, jobGraph.getJobName());

    jobGraph.getJobConfig().putAll(jobConfig);

    // create job master actor
    this.jobMasterActor =
        Ray.actor(JobMaster::new, jobConfig).setResources(resources).setMaxRestarts(-1).remote();

    try {
      ObjectRef<Boolean> submitResult =
          jobMasterActor.task(JobMaster::submitJob, jobMasterActor, jobGraph).remote();

      if (submitResult.get()) {
        LOG.info("Finish submitting job: {}.", jobGraph.getJobName());
      } else {
        throw new RuntimeException("submitting job failed");
      }
    } catch (Exception e) {
      LOG.error("Failed to submit job: {}.", jobGraph.getJobName(), e);
      throw new RuntimeException("submitting job failed", e);
    }
  }
}
