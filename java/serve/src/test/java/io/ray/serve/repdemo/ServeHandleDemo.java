package io.ray.serve.repdemo;

import io.ray.api.ObjectRef;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.handle.RayServeHandle;

// 1. Deployment and Application
public class ServeHandleDemo {
  private RayServeHandle modelAHandle;
  private RayServeHandle modelBHandle;

  public ServeHandleDemo(RayServeHandle modelAHandle, RayServeHandle modelBHandle) {
    this.modelAHandle = modelAHandle;
    this.modelBHandle = modelBHandle;
  }

  public String call(String request) {
    ObjectRef<Object> refA = modelAHandle.remote(request);
    ObjectRef<Object> refB = modelAHandle.remote(request);
    return request;
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
    Deployment deployment =
      Serve.deployment()
        .setDeploymentDef(ServeHandleDemo.class.getName())
        .create();
    Application strategyApp = deployment.bind();
    RayServeHandle handle = Serve.run(strategyApp);
    System.out.println(handle.remote().get());
  }
}

// 2. ServeHandle (composing deployments)
// deployment 的初始化参数可以是serve handle

// 4. Deployment Graph
// DAGDriver