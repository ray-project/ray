package io.ray.serve.docdemo;

// [strategy-start]
public class StrategyOnRayServe {

  public String calcIndicator(long time, String bank, String indicator) {
    // do bank data calculation
    return bank + "-" + indicator + "-" + time; // Demo;
  }
}
// [strategy-end]
