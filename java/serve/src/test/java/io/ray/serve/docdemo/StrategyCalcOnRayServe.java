package io.ray.serve.docdemo;

import io.ray.api.ObjectRef;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalcOnRayServe {

  public void deploy() {
    // Start Ray Serve instance.
    Serve.start(true, false, null, null);

    // Deploy counter.
    Deployment deployment =
        Serve.deployment()
            .setName("strategy")
            .setDeploymentDef(StrategyOnRayServe.class.getName())
            .setNumReplicas(4)
            .create();
    deployment.deploy(true);
  }

  public List<Result> calc(long time, Map<String, BankIndicator> banksAndIndicator) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<Result> results = new ArrayList<>();
    for (Entry<String, BankIndicator> e : banksAndIndicator.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue().getIndicators()) {
        for (String indicator : indicators) {
          results.add(
              (Result)
                  deployment
                      .getHandle()
                      .method("calcIndicator")
                      .remote(time, bank, indicator)
                      .get());
        }
      }
    }
    return results;
  }

  public List<Result> parallelCalc(long time, Map<String, BankIndicator> banksAndIndicator) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<Result> results = new ArrayList<>();
    List<ObjectRef<Object>> refs = new ArrayList<>();
    for (Entry<String, BankIndicator> e : banksAndIndicator.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue().getIndicators()) {
        for (String indicator : indicators) {
          refs.add(deployment.getHandle().method("calcIndicator").remote(time, bank, indicator));
        }
      }
    }
    for (ObjectRef<Object> ref : refs) {
      results.add((Result) ref.get());
    }
    return results;
  }

  public static void main(String[] args) {
    long time = 0;
    Map<String, BankIndicator> banksAndIndicator = null;

    StrategyCalcOnRayServe strategy = new StrategyCalcOnRayServe();
    strategy.deploy();
    strategy.parallelCalc(time, banksAndIndicator);
  }
}
