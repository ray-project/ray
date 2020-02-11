package org.ray.deploy.rps.model.release;

import java.util.Arrays;
import org.ray.deploy.rps.model.ElasticityContainer;
import org.ray.deploy.rps.model.Response;

public class ReleaseResponse extends Response {

  ElasticityContainer[] released;

  public ReleaseResponse() {
  }

  public ElasticityContainer[] getReleased() {
    return released;
  }

  public void setReleased(ElasticityContainer[] released) {
    this.released = released;
  }

  @Override
  public String toString() {
    return "ReleaseResponse{" +
        "released=" + Arrays.toString(released) +
        ", errorCode=" + errorCode +
        ", errorMsg='" + errorMsg + '\'' +
        '}';
  }
}
