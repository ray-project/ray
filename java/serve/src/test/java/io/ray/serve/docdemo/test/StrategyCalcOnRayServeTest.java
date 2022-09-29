package io.ray.serve.docdemo.test;

import io.ray.serve.BaseServeTest;
import io.ray.serve.docdemo.StrategyCalcOnRayServe;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StrategyCalcOnRayServeTest extends BaseServeTest {

  @Test(groups = {"cluster"})
  public void test() {
    String prefix = "StrategyCalcOnRayServeTest";
    String bank1 = prefix + "_bank_1";
    String bank2 = prefix + "_bank_2";
    String indicator1 = prefix + "_indicator_1";
    String indicator2 = prefix + "_indicator_2";
    long time = System.currentTimeMillis();

    Map<String, List<List<String>>> banksAndIndicators = new HashMap<>();
    banksAndIndicators.put(bank1, Arrays.asList(Arrays.asList(indicator1, indicator2)));
    banksAndIndicators.put(
        bank2, Arrays.asList(Arrays.asList(indicator1), Arrays.asList(indicator2)));

    StrategyCalcOnRayServe strategy = new StrategyCalcOnRayServe();
    strategy.deploy();

    List<String> results = strategy.calc(time, banksAndIndicators);
    Assert.assertTrue(results.contains(bank1 + "-" + indicator1 + "-" + time));
    Assert.assertTrue(results.contains(bank1 + "-" + indicator2 + "-" + time));
    Assert.assertTrue(results.contains(bank2 + "-" + indicator1 + "-" + time));
    Assert.assertTrue(results.contains(bank2 + "-" + indicator1 + "-" + time));

    results = strategy.parallelCalc(time, banksAndIndicators);
    Assert.assertTrue(results.contains(bank1 + "-" + indicator1 + "-" + time));
    Assert.assertTrue(results.contains(bank1 + "-" + indicator2 + "-" + time));
    Assert.assertTrue(results.contains(bank2 + "-" + indicator1 + "-" + time));
    Assert.assertTrue(results.contains(bank2 + "-" + indicator1 + "-" + time));
  }
}
