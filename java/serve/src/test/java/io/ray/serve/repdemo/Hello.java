package io.ray.serve.repdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import java.util.Map;

public class Hello {

  public static class HelloWorldArgs {
    private String message;

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }
  }

  public static class HelloWorld {
    private String message;

    public HelloWorld(String message) {
      this.message = message;
    }

    public String call() {
      return message;
    }
  }

  public static Application appBuilder(Map<String, String> args) {
    return Serve.deployment()
        .setDeploymentDef(HelloWorld.class.getName())
        .bind(args.get("message"));
  }

  public static Application typedAppBuilder(HelloWorldArgs args) {
    return Serve.deployment().setDeploymentDef(HelloWorld.class.getName()).bind(args.getMessage());
  }
}
