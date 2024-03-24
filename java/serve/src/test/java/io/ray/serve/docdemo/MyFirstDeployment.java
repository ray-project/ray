package io.ray.serve.docdemo;

// api-deployment-start
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.handle.DeploymentHandle;

public class MyFirstDeployment {
  private String msg;

  public MyFirstDeployment(String msg) {
    this.msg = msg;
  }

  public String call() {
    return msg;
  }

  public static void main(String[] args) {
    Application deployment =
        Serve.deployment().setDeploymentDef(MyFirstDeployment.class.getName()).bind("Hello world!");
    DeploymentHandle handle = Serve.run(deployment).get();
    System.out.println(handle.remote().result());
  }
}
// api-deployment-end
