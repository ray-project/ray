package io.ray.serve.docdemo;

import java.util.Map;

public class StrategyCalc {

  public static void main(String[] args) {
    long time = 0;
    Map<String, BankIndicator> banksAndIndicator = null;

    Strategy strategy = new Strategy();
    strategy.calc(time, banksAndIndicator);
  }
}
