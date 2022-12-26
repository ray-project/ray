package io.ray.serve.docdemo;

import com.google.gson.Gson;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hc.client5.http.fluent.Request;

public class HttpStrategyCalcOnRayServe {

  public void deploy() {
    Serve.start(true, false, null);

    Deployment deployment =
        Serve.deployment()
            .setName("http-strategy")
            .setDeploymentDef(HttpStrategyOnRayServe.class.getName())
            .setNumReplicas(4)
            .create();
    deployment.deploy(true);
  }

  // docs-http-start
  private Gson gson = new Gson();

  public String httpCalc(long time, String bank, String indicator) {
    Map<String, Object> data = new HashMap<>();
    data.put("time", time);
    data.put("bank", bank);
    data.put("indicator", indicator);

    String result;
    try {
      result =
          Request.post("http://127.0.0.1:8000/strategy")
              .bodyString(gson.toJson(data), null)
              .addHeader("Content-Type", "application/json")
              .execute()
              .returnContent()
              .asString();
    } catch (IOException e) {
      result = "error";
    }

    return result;
  }
  // docs-http-end

  // docs-calc-start
  public List<String> calc(long time, Map<String, List<List<String>>> banksAndIndicators) {

    List<String> results = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          results.add(httpCalc(time, bank, indicator));
        }
      }
    }
    return results;
  }
  // docs-calc-end

  // docs-parallel-calc-start
  private ExecutorService executorService = Executors.newFixedThreadPool(4);

  public List<String> parallelCalc(long time, Map<String, List<List<String>>> banksAndIndicators) {

    List<String> results = new ArrayList<>();
    List<Future<String>> futures = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          futures.add(executorService.submit(() -> httpCalc(time, bank, indicator)));
        }
      }
    }
    for (Future<String> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException | ExecutionException e1) {
        results.add("error");
      }
    }
    return results;
  }
  // docs-parallel-calc-end

  // docs-main-start
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

    HttpStrategyCalcOnRayServe strategy = new HttpStrategyCalcOnRayServe();
    strategy.deploy();
    List<String> results = strategy.parallelCalc(time, banksAndIndicators);

    System.out.println(results);
  }
  // docs-main-end
}
