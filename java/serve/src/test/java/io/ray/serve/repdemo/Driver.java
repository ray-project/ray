package io.ray.serve.repdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.handle.DeploymentResponse;

public class Driver {
  private DeploymentHandle modelAHandle;
  private DeploymentHandle modelBHandle;

  public Driver(DeploymentHandle modelAHandle, DeploymentHandle modelBHandle) {
    this.modelAHandle = modelAHandle;
    this.modelBHandle = modelBHandle;
  }

  public String call(String request) {
    DeploymentResponse responseA = modelAHandle.remote(request);
    DeploymentResponse responseB = modelBHandle.remote(request);
    return (String) responseA.result() + responseB.result();
  }

  public static class ModelA {
    public String call(String msg) {
      return msg;
    }
  }

  public static class ModelB {
    public String call(String msg) {
      return msg;
    }
  }

  public static void main(String[] args) {
    Application modelA = Serve.deployment().setDeploymentDef(ModelA.class.getName()).bind();
    Application modelB = Serve.deployment().setDeploymentDef(ModelB.class.getName()).bind();

    Application driver =
        Serve.deployment().setDeploymentDef(Driver.class.getName()).bind(modelA, modelB);
    Serve.run(driver);
  }
}
