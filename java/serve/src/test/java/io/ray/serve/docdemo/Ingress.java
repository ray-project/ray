package io.ray.serve.docdemo;

// api-composing-start
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.handle.DeploymentHandle;

public class Ingress {

  public static class Hello {
    public String call() {
      return "Hello";
    }
  }

  public static class World {
    public String call() {
      return " world!";
    }
  }

  private DeploymentHandle helloHandle;
  private DeploymentHandle worldHandle;

  public Ingress(DeploymentHandle helloHandle, DeploymentHandle worldHandle) {
    this.helloHandle = helloHandle;
    this.worldHandle = worldHandle;
  }

  public String call() {
    return (String) helloHandle.remote().result() + worldHandle.remote().result();
  }

  public static void main(String[] args) {
    Application hello = Serve.deployment().setDeploymentDef(Hello.class.getName()).bind();
    Application world = Serve.deployment().setDeploymentDef(World.class.getName()).bind();

    Application app =
        Serve.deployment().setDeploymentDef(Ingress.class.getName()).bind(hello, world);

    DeploymentHandle handle = Serve.run(app).get();

    System.out.println(handle.remote().result());
  }
}
// api-composing-end
