package io.ray.serve.docdemo;

// docs-strategy-start
import com.google.gson.Gson;

public class HttpStrategyOnRayServe {

  static class BankIndicator {
    long time;
    String bank;
    String indicator;
  }

  private Gson gson = new Gson();

  public String call(String dataJson) {
    BankIndicator data = gson.fromJson(dataJson, BankIndicator.class);
    // do bank data calculation
    return data.bank + "-" + data.indicator + "-" + data.time; // Demo;
  }
}
// docs-strategy-end
