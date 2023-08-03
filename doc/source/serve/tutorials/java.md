(serve-java-tutorial)=

# Java Tutorial

To use Java Ray Serve, you need the following dependency in your pom.xml.

```xml
<dependency>
  <groupId>io.ray</groupId>
  <artifactId>ray-serve</artifactId>
  <version>${ray.version}</version>
  <scope>provided</scope>
</dependency>
```

> NOTE: After installing Ray via Python, the Java jar of Ray Serve is included locally. The `provided` scope could ensure the Java code using Ray Serve can be compiled and will not cause version conflicts when deployed on the cluster.

## Example Model

Our example use case is derived from production workflow of a financial application. The application needs to compute the best strategy to interact with different banks for a single task.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/Strategy.java
:end-before: docs-strategy-end
:language: java
:start-after: docs-strategy-start
```

This `Strategy` class is used to calculate the indicators of a number of banks.

* The `calc` method is the entry of the calculation. The input parameters are the time interval of calculation and the map of the banks and their indicators. As we can see, the `calc` method contains a two-tier `for` loop, traversing each indicator list of each bank, and calling the `calcBankIndicators` method to calculate the indicators of the specified bank.

- There is another layer of `for` loop in the `calcBankIndicators` method, which traverses each indicator, and then calls the `calcIndicator` method to calculate the specific indicator of the bank.
- The `calcIndicator` method is a specific calculation logic based on the bank, the specified time interval and the indicator.

This is the code that uses the `Strategy` class:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalc.java
:end-before: docs-strategy-calc-end
:language: java
:start-after: docs-strategy-calc-start
```

When the scale of banks and indicators expands, the three-tier `for` loop will slow down the calculation. Even if the thread pool is used to calculate each indicator in parallel, we may encounter a single machine performance bottleneck. Moreover, this `Strategy`  object cannot be reused as a resident service.

## Converting to a Ray Serve Deployment

Through Ray Serve, the core computing logic of `Strategy` can be deployed as a scalable distributed computing service.

First, we can extract the indicator calculation of each institution into a separate `StrategyOnRayServe` class:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyOnRayServe.java
:end-before: docs-strategy-end
:language: java
:start-after: docs-strategy-start
```

Next, we start the Ray Serve runtime and deploy `StrategyOnRayServe` as a deployment.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-deploy-end
:language: java
:start-after: docs-deploy-start
```

The `Deployment.create` makes a Deployment object named "strategy." After executing `Deployment.deploy`, this "strategy" deployment is deployed in the instance of Ray Serve with four replicas, and we can access it for distributed parallel computing.

## Testing the Ray Serve Deployment

Now we can test the "strategy" deployment using RayServeHandle inside Ray:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-calc-end
:language: java
:start-after: docs-calc-start
```

At present, the calculation of each bank's each indicator is still executed serially, and sent to Ray for execution. We can make the calculation concurrent, which not only improves the calculation efficiency, but also solves the bottleneck of single machine.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-parallel-calc-end
:language: java
:start-after: docs-parallel-calc-start
```

Now, we can use `StrategyCalcOnRayServe` like the example in the `main` method:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-main-end
:language: java
:start-after: docs-main-start
```

## Calling Ray Serve Deployment with HTTP

Another way to test or call a deployment is through the HTTP request. But there are now two limitations for the Java deployments:

- The HTTP requests can only be processed by the `call` method of the user class.

- The `call` method could only have one input parameter, and the type of the input parameter and the returned value can only be `String`.

If we want to call the "strategy" deployment via HTTP, the class can be rewritten like this:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyOnRayServe.java
:end-before: docs-strategy-end
:language: java
:start-after: docs-strategy-start
```

After deploying this deployment, we can access it through `curl` command:

```shell
curl -d '{"time":1641038674, "bank":"test_bank", "indicator":"test_indicator"}' http://127.0.0.1:8000/strategy
```

It can also be accessed using HTTP Client in Java code:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyCalcOnRayServe.java
:end-before: docs-http-end
:language: java
:start-after: docs-http-start
```

The example of strategy calculation using HTTP to access deployment is as follows:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyCalcOnRayServe.java
:end-before: docs-calc-end
:language: java
:start-after: docs-calc-start
```

This code can also be rewritten to support concurrency:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyCalcOnRayServe.java
:end-before: docs-parallel-calc-end
:language: java
:start-after: docs-parallel-calc-start
```

Finally, the complete usage of `HttpStrategyCalcOnRayServe` is like this:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyCalcOnRayServe.java
:end-before: docs-main-end
:language: java
:start-after: docs-main-start
```
