package org.ray.deploy.rps.model.allocate;

import java.util.Arrays;
import org.ray.deploy.rps.model.ElasticityContainer;
import org.ray.deploy.rps.model.Response;

public class AllocateResponse extends Response {

  ElasticityContainer[] allocated;

  public AllocateResponse() {
  }

  public ElasticityContainer[] getAllocated() {
    return allocated;
  }

  public void setAllocated(ElasticityContainer[] allocated) {
    this.allocated = allocated;
  }

  @Override
  public String toString() {
    return "AllocateResponse{" +
        "allocated=" + Arrays.toString(allocated) +
        ", errorCode=" + errorCode +
        ", errorMsg='" + errorMsg + '\'' +
        '}';
  }
}
