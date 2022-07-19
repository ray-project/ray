package io.ray.serve.docdemo;

import com.google.gson.Gson;

public class HttpStrategyOnRayServe {

  private Gson gson = new Gson();

  public String calcIndicator(String indicatorJson) {
    IndicatorModel indicatorModel = gson.fromJson(indicatorJson, IndicatorModel.class);
  	long time = indicatorModel.getTime();
  	String bank = indicatorModel.getBank();
  	String indicator = indicatorModel.getIndicator();
    Result result = new Result();
    // do bank data calculation
    return gson.toJson(result);
  }
}
