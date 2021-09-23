package io.ray.serve.util;

public class ExampleEchoDeployment {
   String prefix;

  public ExampleEchoDeployment(String prefix) {
    this.prefix = prefix;
  }

  public String call(String input) {
     return this.prefix + input;
   }
}
