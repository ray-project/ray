(getting-started-java)=

# Getting Started

First of all, using Java Ray Serve needs the following dependency in you `pom.xml`:

```xml
<dependency>
  <groupId>io.ray</groupId>
  <artifactId>ray-serve</artifactId>
  <version>${ray.version}</version>
  <scope>provided</scope>
</dependency>
```

.. note::

After installing Ray, the Java jar of Ray Serve has been included locally. The `provided` scope could  ensure the Java code using Ray Serve can be compiled and will not cause version conflicts when deployed on the cluster.


## Example Model

This is a real user scenario we encountered. The use's class is as follows:

```java
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

```

The `Strategy` class is used to calculate the indicators of a number of banks.

* The `calc` method is the entry of the calculation. The input parameters are the time interval of calculation and the map of the banks and their indicators. As we can see, the `calc` method contains a two-tier `for` loop, traversing each indicator list of each bank, and calling the `calcBankIndicators` method to calculate the indicators of the specified bank.

- There is another layer of `for` loop in the `calcBankIndicators` method, which traverses each indicator, and then calls the `calcIndicator` method to calculate the specific indicator of the bank.
- The `calcIndicator` method is a specific calculation logic based on the bank, the specified time interval and the indicator.

This is the code that uses the `Strategy` class:

```java
Strategy strategy = new Strategy();
strategy.calc(time, banksAndIndicator);
```

When the scale of banks and indicators expands, the three-tier `for` loop will slow down the calculation. Even if the thread pool is used to calculate each indicator in parallel, we may encounter a single machine performance bottleneck. Moreover, this `Strategy`  object cannot be reused as a resident service.

## Converting to a Ray Serve Deployment

Through Ray Serve, the core computing logic of `Strategy` can be deployed as a scalable distributed computing service.

First, we can extract the indicator calculation of each institution into a separate `StrategyOnRayServe` class:

```java
public class StrategyOnRayServe {

  public Result calcIndicator(long time, String bank, String indicator) {
    Result result = new Result();
    // do bank data calculation
    return result;
  }
}

```

Next, we start the Ray Serve runtime and deploy `StrategyService` as a deployment.

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

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
}

```

The `Deployment.create` makes a `Deployment` object named "strategy". After executing `Deployment.deploy`, this "strategy" deployment is deployed in the instance of Ray Serve with 4 replicas, and we can access it for distributed parallel computing.

## Testing the Ray Serve Deployment

Now we can test the "strategy" deployment using RayServeHandle inside Ray:

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalcOnRayServe {

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
}
```

At present, the calculation of each bank's each indicator is still in series, but only send the calculation task to ray for execution. We can change the calculation to concurrency, which not only improves the calculation efficiency, but also solves the bottleneck of single machine.

```java
import io.ray.api.ObjectRef;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalcOnRayServe {

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
}
```

Now, we can use `StrategyCalcOnRayServe` like this:

```java
StrategyCalcOnRayServe strategy = new StrategyCalcOnRayServe();
strategy.deploy();
strategy.parallelCalc(time, banksAndIndicator);
```

## Calling Ray Serve Deployment with HTTP

For a deployment that has been deployed on Ray Serve, another way to call it is through the HTTP request. But there are two limitations now in Java Ray Serve:

(1) The HTTP requests can only be processed by the `call` method of the user class.

(2) The `call` method could only have one input parameter, and the type of the input parameter and the returned value can only be string type.

If we want to call the "strategy" deployment via HTTP, the class can be rewritten like this: 

```java
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

```

After deploying this deployment, we can access it through `curl` command:

```shell
curl -d '{"time":1641038674, "bank":"test_bank", "indicator":"test_indicator"}' http://127.0.0.1:8000/strategy
```

It can also be accessed using HTTP Client in Java code:

```java
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

```

