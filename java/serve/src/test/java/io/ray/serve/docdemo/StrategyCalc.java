package io.ray.serve.docdemo;

// docs-strategy-calc-start
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StrategyCalc {

  public static void main(String[] args) {
    long time = System.currentTimeMillis();
    String bank1 = "demo_bank_1";
    String bank2 = "demo_bank_2";
    String indicator1 = "demo_indicator_1";
    String indicator2 = "demo_indicator_2";
    Map<String, List<List<String>>> banksAndIndicators = new HashMap<>();
    banksAndIndicators.put(bank1, Arrays.asList(Arrays.asList(indicator1, indicator2)));
    banksAndIndicators.put(
        bank2, Arrays.asList(Arrays.asList(indicator1), Arrays.asList(indicator2)));

    Strategy strategy = new Strategy();
    List<String> results = strategy.calc(time, banksAndIndicators);

    System.out.println(results);
  }
}
// docs-strategy-calc-end
