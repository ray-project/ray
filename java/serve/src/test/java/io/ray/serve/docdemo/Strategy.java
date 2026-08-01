package io.ray.serve.docdemo;

// docs-strategy-start
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Strategy {

  public List<String> calc(Long time, Map<String, List<List<String>>> banksAndIndicators) {
    List<String> results = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        results.addAll(calcBankIndicators(time, bank, indicators));
      }
    }
    return results;
  }

  public List<String> calcBankIndicators(Long time, String bank, List<String> indicators) {
    List<String> results = new ArrayList<>();
    for (String indicator : indicators) {
      results.add(calcIndicator(time, bank, indicator));
    }
    return results;
  }

  public String calcIndicator(Long time, String bank, String indicator) {
    // do bank data calculation
    return bank + "-" + indicator + "-" + time; // Demo;
  }
}
// docs-strategy-end
