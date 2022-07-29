package io.ray.serve.docdemo;

public class StrategyOnRayServe {

  public String calcIndicator(long time, String bank, String indicator) {
    // do bank data calculation
    return bank + "-" + indicator + "-" + time; // Demo;
  }
}
