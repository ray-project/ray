package io.ray.serve.docdemo;

// docs-strategy-start
import com.google.gson.Gson;
import java.util.Map;

public class HttpStrategyOnRayServe {

  private Gson gson = new Gson();

  public String call(String dataJson) {
    Map<String, Object> data = gson.fromJson(dataJson, Map.class);
    long time = (long) data.get("time");
    String bank = (String) data.get("bank");
    String indicator = (String) data.get("indicator");
    // do bank data calculation
    return bank + "-" + indicator + "-" + time; // Demo;
  }
}
// docs-strategy-end
