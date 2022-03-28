package io.ray.test;

public class ExampleImpl implements ExampleInterface {

  @Override
  public String echo(String str) {
    return str;
  }

  public String overloadedFunc(String str1) {
    return str1;
  }

  public String overloadedFunc(String str1, String str2) {
    return str1 + str2;
  }
}
