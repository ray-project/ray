package io.ray.serve.docdemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Strategy {

  public List<Result> calc(long time, Map<String, BankIndicator> banksAndIndicator) {
    List<Result> results = new ArrayList<>();
    for (Entry<String, BankIndicator> e : banksAndIndicator.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue().getIndicators()) {
        results.addAll(calcBankIndicators(time, bank, indicators));
      }
    }
    return results;
  }

  public List<Result> calcBankIndicators(long time, String bank, List<String> indicators) {
    List<Result> results = new ArrayList<>();
    for (String indicator : indicators) {
      results.add(calcIndicator(time, bank, indicator));
    }
    return results;
  }

  public Result calcIndicator(long time, String bank, String indicator) {
    Result result = new Result();
    // do bank data calculation
    return result;
  }
}
