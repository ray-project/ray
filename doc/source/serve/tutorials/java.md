(serve-java-tutorial)=

# Java Tutorial

First of all, using Java Ray Serve needs the following dependency in you `pom.xml`:

```xml
<dependency>
  <groupId>io.ray</groupId>
  <artifactId>ray-serve</artifactId>
  <version>${ray.version}</version>
  <scope>provided</scope>
</dependency>
```

> NOTE: After installing Ray, the Java jar of Ray Serve has been included locally. The `provided` scope could  ensure the Java code using Ray Serve can be compiled and will not cause version conflicts when deployed on the cluster.

## Example Model

This is a real user scenario we encountered. The user's class is as follows:

```java
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Strategy {

  public List<String> calc(long time, Map<String, List<List<String>>> banksAndIndicators) {
    List<String> results = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        results.addAll(calcBankIndicators(time, bank, indicators));
      }
    }
    return results;
  }

  public List<String> calcBankIndicators(long time, String bank, List<String> indicators) {
    List<String> results = new ArrayList<>();
    for (String indicator : indicators) {
      results.add(calcIndicator(time, bank, indicator));
    }
    return results;
  }

  public String calcIndicator(long time, String bank, String indicator) {
    // do bank data calculation
    return bank + "-" + indicator + "-" + time; // Demo;
  }
}

```

This `Strategy` class is used to calculate the indicators of a number of banks.

* The `calc` method is the entry of the calculation. The input parameters are the time interval of calculation and the map of the banks and their indicators. As we can see, the `calc` method contains a two-tier `for` loop, traversing each indicator list of each bank, and calling the `calcBankIndicators` method to calculate the indicators of the specified bank.

- There is another layer of `for` loop in the `calcBankIndicators` method, which traverses each indicator, and then calls the `calcIndicator` method to calculate the specific indicator of the bank.
- The `calcIndicator` method is a specific calculation logic based on the bank, the specified time interval and the indicator.

This is the code that uses the `Strategy` class:

```java
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StrategyCalc {

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

    Strategy strategy = new Strategy();
    List<String> results = strategy.calc(time, banksAndIndicators);

    System.out.println(results);
  }
}

```

When the scale of banks and indicators expands, the three-tier `for` loop will slow down the calculation. Even if the thread pool is used to calculate each indicator in parallel, we may encounter a single machine performance bottleneck. Moreover, this `Strategy`  object cannot be reused as a resident service.

## Converting to a Ray Serve Deployment

Through Ray Serve, the core computing logic of `Strategy` can be deployed as a scalable distributed computing service.

First, we can extract the indicator calculation of each institution into a separate `StrategyOnRayServe` class:

```java
public class StrategyOnRayServe {

  public String calcIndicator(long time, String bank, String indicator) {
    // do bank data calculation
    return bank + "-" + indicator + "-" + time; // Demo;
  }
}

```

Next, we start the Ray Serve runtime and deploy `StrategyService` as a deployment.

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class StrategyCalcOnRayServe {

  public void deploy() {
    Serve.start(true, false, null, null);

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

  public List<String> calc(long time, Map<String, List<List<String>>> banksAndIndicators) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<String> results = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          results.add(
              (String)
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

At present, the calculation of each bank's each indicator is still in series, and just sended to Ray for execution. We can make the calculation concurrent, which not only improves the calculation efficiency, but also solves the bottleneck of single machine.

```java
import io.ray.api.ObjectRef;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalcOnRayServe {

  public List<String> parallelCalc(long time, Map<String, List<List<String>>> banksAndIndicators) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<String> results = new ArrayList<>();
    List<ObjectRef<Object>> refs = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          refs.add(deployment.getHandle().method("calcIndicator").remote(time, bank, indicator));
        }
      }
    }
    for (ObjectRef<Object> ref : refs) {
      results.add((String) ref.get());
    }
    return results;
  }
}
```

Now, we can use `StrategyCalcOnRayServe` like the example in the `main` method:

```java
import io.ray.api.ObjectRef;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalcOnRayServe {

  public void deploy() {
    Serve.start(true, false, null, null);

    Deployment deployment =
        Serve.deployment()
            .setName("strategy")
            .setDeploymentDef(StrategyOnRayServe.class.getName())
            .setNumReplicas(4)
            .create();
    deployment.deploy(true);
  }

  public List<String> calc(long time, Map<String, List<List<String>>> banksAndIndicators) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<String> results = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          results.add(
              (String)
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

  public List<String> parallelCalc(long time, Map<String, List<List<String>>> banksAndIndicators) {
    Deployment deployment = Serve.getDeployment("strategy");

    List<String> results = new ArrayList<>();
    List<ObjectRef<Object>> refs = new ArrayList<>();
    for (Entry<String, List<List<String>>> e : banksAndIndicators.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue()) {
        for (String indicator : indicators) {
          refs.add(deployment.getHandle().method("calcIndicator").remote(time, bank, indicator));
        }
      }
    }
    for (ObjectRef<Object> ref : refs) {
      results.add((String) ref.get());
    }
    return results;
  }

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

    StrategyCalcOnRayServe strategy = new StrategyCalcOnRayServe();
    strategy.deploy();
    List<String> results = strategy.parallelCalc(time, banksAndIndicators);

    System.out.println(results);
  }
}

```

## Calling Ray Serve Deployment with HTTP

Another way to call a deployment is through the HTTP request. But there are now two limitations for the Java deployments:

- The HTTP requests can only be processed by the `call` method of the user class.

- The `call` method could only have one input parameter, and the type of the input parameter and the returned value can only be `String`.

If we want to call the "strategy" deployment via HTTP, the class can be rewritten like this: 

```java
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

```

After deploying this deployment, we can access it through `curl` command:

```shell
curl -d '{"time":1641038674, "bank":"test_bank", "indicator":"test_indicator"}' http://127.0.0.1:8000/strategy
```

It can also be accessed using HTTP Client in Java code:

```java
import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hc.client5.http.fluent.Request;

public class HttpStrategyCalcOnRayServe {

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
              .execute()
              .returnContent()
              .asString();
    } catch (IOException e) {
      result = "error";
    }

    return result;
  }
}

```

The example of strategy calculation using HTTP to access deployment is as follows:

```java
package io.ray.serve.docdemo;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hc.client5.http.fluent.Request;

public class HttpStrategyCalcOnRayServe {

  private Gson gson = new Gson();

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
              .execute()
              .returnContent()
              .asString();
    } catch (IOException e) {
      result = "error";
    }

    return result;
  }
}
```

This code can also be rewritten to support concurrency:

```java
package io.ray.serve.docdemo;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
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

  private Gson gson = new Gson();

  private ExecutorService executorService = Executors.newFixedThreadPool(4);;

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
              .execute()
              .returnContent()
              .asString();
    } catch (IOException e) {
      result = "error";
    }

    return result;
  }
}
```

Now, the complete usage of `HttpStrategyCalcOnRayServe` is like this:

```java
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

  private Gson gson = new Gson();

  private ExecutorService executorService = Executors.newFixedThreadPool(4);;

  public void deploy() {
    Serve.start(true, false, null, null);

    Deployment deployment =
        Serve.deployment()
            .setName("http-strategy")
            .setDeploymentDef(HttpStrategyOnRayServe.class.getName())
            .setNumReplicas(4)
            .create();
    deployment.deploy(true);
  }

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
              .execute()
              .returnContent()
              .asString();
    } catch (IOException e) {
      result = "error";
    }

    return result;
  }

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
}

```