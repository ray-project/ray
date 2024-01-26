package io.ray.serve.docdemo.test;

import com.google.gson.Gson;
import io.ray.serve.BaseServeTest;
import io.ray.serve.docdemo.HttpStrategyCalcOnRayServe;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hc.client5.http.fluent.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HttpStrategyCalcOnRayServeTest extends BaseServeTest {

  public static class HttpStrategyCalcOnRayServeForTest extends HttpStrategyCalcOnRayServe {

    private Gson gson = new Gson();

    @Override
    public String httpCalc(Long time, String bank, String indicator) {
      Map<String, Object> data = new HashMap<>();
      data.put("time", time);
      data.put("bank", bank);
      data.put("indicator", indicator);
      String result;
      try {
        result =
            Request.post("http://127.0.0.1:8341/http-strategy")
                .bodyString(gson.toJson(data), null)
                .execute()
                .returnContent()
                .asString();
      } catch (IOException e) {
        result = "error";
      }
      return result;
    }
  }

  @Test(groups = {"cluster"})
  public void test() {
    String prefix = "HttpStrategyCalcOnRayServeTest";
    String bank1 = prefix + "_bank_1";
    String bank2 = prefix + "_bank_2";
    String indicator1 = prefix + "_indicator_1";
    String indicator2 = prefix + "_indicator_2";
    long time = System.currentTimeMillis();

    Map<String, List<List<String>>> banksAndIndicators = new HashMap<>();
    banksAndIndicators.put(bank1, Arrays.asList(Arrays.asList(indicator1, indicator2)));
    banksAndIndicators.put(
        bank2, Arrays.asList(Arrays.asList(indicator1), Arrays.asList(indicator2)));

    HttpStrategyCalcOnRayServe strategy = new HttpStrategyCalcOnRayServeForTest();
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
