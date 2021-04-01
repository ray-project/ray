package io.ray.performancetest;

import com.google.common.base.Preconditions;
import io.ray.api.Ray;
import io.ray.api.call.BaseActorCreator;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobInfo.class);

  public String jobName;
  public List<NodeInfo> nodes;

  public static JobInfo parseJobInfo(String[] args) {
    for (String arg : args) {
      LOGGER.info("arg: {}", arg);
    }

    JobInfo jobInfo = new JobInfo();
    if (TestUtils.isDevMode()) {
      jobInfo.jobName = "dev-job-name";
    } else {
      Preconditions.checkState(args.length >= 1);
      jobInfo.jobName = args[0];
    }

    Preconditions.checkNotNull(jobInfo.jobName);
    String namespaceId = System.getenv("namespaceId");
    LOGGER.info("namespaceId: {}", namespaceId);

    return jobInfo;
  }

  public <T extends BaseActorCreator> T assignActorToNode(
      BaseActorCreator<T> actorCreator, int nodeIndex) {
    if (TestUtils.isDevMode()) {
      return (T) actorCreator;
    }
    Preconditions.checkState(
        nodes.size() > nodeIndex,
        "Node index " + nodeIndex + " is out of range. Total nodes: " + nodes.size() + ".");
    return actorCreator.setResource("Node" + nodeIndex, 1.0);
  }
}
