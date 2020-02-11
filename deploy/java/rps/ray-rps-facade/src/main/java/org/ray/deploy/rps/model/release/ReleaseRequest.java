package org.ray.deploy.rps.model.release;

import java.util.Arrays;
import org.ray.deploy.rps.model.ElasticityContainer;
import org.ray.deploy.rps.model.Request;

public class ReleaseRequest extends Request {

  ElasticityContainer[] releaseContainers;

  public ReleaseRequest() {
  }

  public ElasticityContainer[] getReleaseContainers() {
    return releaseContainers;
  }

  public void setReleaseContainers(ElasticityContainer[] releaseContainers) {
    this.releaseContainers = releaseContainers;
  }

  @Override
  public String toString() {
    return "ReleaseRequest{" +
        "releaseContainers=" + Arrays.toString(releaseContainers) +
        ", clusterName='" + clusterName + '\'' +
        ", sequence=" + sequence +
        '}';
  }
}
