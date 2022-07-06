(getting-started-java)=

# Getting Started

First of all, using Java Ray Serve needs the following dependencys in you `pom.xml`:

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

This is a real user scenario we encountered. The user's class is as follows:

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
      // do indicators' data calculation
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

The `Strategy` Class is used to calculate the indicators of the banks.

* The `calc` method is the entry of the calculation. The input parameters are the time interval of calculation and the map of the banks and their indicators. The `calc` contains a two-tier `for` loop, traversing each indicator list of each bank, and calling the `calcBankIndicators` method to calculate the indicators of the specified bank.

- There is another layer of `for` loop in the `calcBankIndicators` method, which traverses each indicator, and then calls the `calcIndicator` method to calculate the specific indicator of the bank.
- The `calcIndicator` method is a specific calculation logic based on the bank, the specified time interval and the indicator.



With the increase of the number of the payment institutions and the indicators, the single machine bottleneck will be encountered when using this class directly:

```java
Strategy strategy = new Strategy();
strategy.calc(time, banksAndIndicator);
```



Moreover, this `Strategy`  object cannot be reused as a resident service.



## Converting to a Ray Serve Deployment

Through Ray Serve, the core computing logic of `Strategy` can be deployed as a distributed computing service, which can be scaled and accessed through HTTP. Users only need to call the service for parallel computing.

First, we can extract the indicator calculation of each institution into a separate class:

```java
public class StrategyService {

  public Result calcIndicator(long time, String bank, String indicator) {
    Result result = new Result();
    // do bank data calculation
    return result;
  }
}
```



Next, we start the Ray Serve runtime and deploy `StrategyService` as a deployment:

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class StrategyCalc {

  public static void deploy() {
    // Start Ray Serve instance.
    Serve.start(true, false, null, null);

    // Deploy counter.
    Deployment deployment =
        Serve.deployment()
            .setName("calc")
            .setDeploymentDef(StrategyService.class.getName())
            .setNumReplicas(2)
            .create();
    deployment.deploy(true);
  }
}
```



After executing `deployment.deploy`, a service named "calc" is deployed in the instance of Ray Serve with two replicas, and we can access this service for distributed parallel computing.



## Testing the Ray Serve Deployment

The deployed deployment can be called through RayServeHandle inside Ray:

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class StrategyCalc {

  public List<Result> calc(long time, Map<String, BankIndicator> banksAndIndicator) {
    List<Result> results = new ArrayList<>();
    Deployment deployment = Serve.getDeployment("calc");
    for (Entry<String, BankIndicator> e : banksAndIndicator.entrySet()) {
      String bank = e.getKey();
      for (List<String> indicators : e.getValue().getIndicators()) {
        for (String indicator : indicators) {
          results.add(
              (Result)
                  deployment.getHandle().method("calcIndicator").remote(time, bank, indicator));
        }
      }
    }
    return results;
  }
}
```



For a deployment that has been deployed on Ray Serve, the most intuitive is to call it through HTTP request. But HTTP currently has two limitations:

(1) You can only call the `call` method of a class deployed as deployment.

(2) The `call` methods can only be single input parameters, and only `String` type is supported.



Here is an example Class `Couter`:

```java
import java.util.concurrent.atomic.AtomicInteger;

public class Counter {

  private AtomicInteger count;

  public Counter(Integer value) {
    this.count = new AtomicInteger(value);
  }

  public String call(String delta) {
    return String.valueOf(count.addAndGet(Integer.valueOf(delta)));
  }
}

```



We deploy it as a Ray Serve Deployment:

```java
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class ServeCounter {

  public static void deploy() {

    // Start Ray Serve instance.
    Serve.start(true, false, null, null);

    // Deploy counter.
    Deployment deployment =
        Serve.deployment()
            .setName("counter")
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(2)
            .setInitArgs(new Object[] {10})
            .create();
    deployment.deploy(true);
  }
}

```



Then it can be accessed through curl commond:

```shell
curl -d '2' http://127.0.0.1:8000/counter
```



Or a line of Java code with HTTP Client:

```java
  public static void http() throws IOException {

    String result =
        Request.post("http://127.0.0.1:8000/counter")
            .bodyString("7", null)
            .execute()
            .returnContent()
            .asString();
    System.out.println(result);
  }
```

