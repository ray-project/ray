package io.ray.serve.docdemo;

import com.google.gson.Gson;
import java.io.IOException;
import org.apache.hc.client5.http.fluent.Request;

public class HttpStrategyCalcOnRayServe {

  private Gson gson = new Gson();

  public Result httpCalc(long time, String bank, String indicator) throws IOException {
    IndicatorModel indicatorModel = new IndicatorModel();
    indicatorModel.setTime(time);
    indicatorModel.setBank(bank);
    indicatorModel.setIndicator(indicator);

    String resultJson =
        Request.post("http://127.0.0.1:8000/strategy")
            .bodyString(gson.toJson(indicatorModel), null)
            .execute()
            .returnContent()
            .asString();
    return gson.fromJson(resultJson, Result.class);
  }
}
