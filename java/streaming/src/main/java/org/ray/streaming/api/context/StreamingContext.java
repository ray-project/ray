package org.ray.streaming.api.context;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.ray.api.Ray;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanBuilder;
import org.ray.streaming.schedule.IJobSchedule;
import org.ray.streaming.schedule.impl.JobScheduleImpl;

/**
 * Encapsulate the context information of a streaming Job.
 */
public class StreamingContext implements Serializable {

  private transient AtomicInteger idGenerator;
  /**
   * The sinks of this streaming job.
   */
  private List<StreamSink> streamSinks;
  private Map<String, Object> jobConfig;
  /**
   * The logic plan.
   */
  private Plan plan;

  private StreamingContext() {
    this.idGenerator = new AtomicInteger(0);
    this.streamSinks = new ArrayList<>();
    this.jobConfig = new HashMap();
  }

  public static StreamingContext buildContext() {
    Ray.init();
    return new StreamingContext();
  }

  /**
   * Construct job DAG, and execute the job.
   */
  public void execute() {
    PlanBuilder planBuilder = new PlanBuilder(this.streamSinks);
    this.plan = planBuilder.buildPlan();
    plan.printPlan();

    IJobSchedule jobSchedule = new JobScheduleImpl(jobConfig);
    jobSchedule.schedule(plan);
  }

  public int generateId() {
    return this.idGenerator.incrementAndGet();
  }

  public void addSink(StreamSink streamSink) {
    streamSinks.add(streamSink);
  }

  public void withConfig(Map<String, Object> jobConfig) {
    this.jobConfig = jobConfig;
  }
}
