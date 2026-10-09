package io.ray.test;

public interface ExampleInterface {

  default String echo(String str) {
    return "default" + str;
  }
}
