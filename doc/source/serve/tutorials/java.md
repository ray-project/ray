(serve-java-tutorial)=

# Serve a Java App

To use Java Ray Serve, you need the following dependency in your pom.xml.

```xml
<dependency>
  <groupId>io.ray</groupId>
  <artifactId>ray-serve</artifactId>
  <version>${ray.version}</version>
  <scope>provided</scope>
</dependency>
```

> NOTE: After installing Ray with Python, the local environment includes the Java jar of Ray Serve. The `provided` scope ensures that you can compile the Java code using Ray Serve without version conflicts when you deploy on the cluster.

## Example model

This example use case is a production workflow of a financial application. The application needs to compute the best strategy to interact with different banks for a single task.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/Strategy.java
:end-before: docs-strategy-end
:language: java
:start-after: docs-strategy-start
```

This example uses the `Strategy` class to calculate the indicators of a number of banks.

* The `calc` method is the entry of the calculation. The input parameters are the time interval of calculation and the map of the banks and their indicators. The `calc` method contains a two-tier `for` loop, traversing each indicator list of each bank, and calling the `calcBankIndicators` method to calculate the indicators of the specified bank.

- There is another layer of `for` loop in the `calcBankIndicators` method, which traverses each indicator, and then calls the `calcIndicator` method to calculate the specific indicator of the bank.
- The `calcIndicator` method is a specific calculation logic based on the bank, the specified time interval and the indicator.

This code uses the `Strategy` class:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalc.java
:end-before: docs-strategy-calc-end
:language: java
:start-after: docs-strategy-calc-start
```

When the scale of banks and indicators expands, the three-tier `for` loop slows down the calculation. Even if you use the thread pool to calculate each indicator in parallel, you may encounter a single machine performance bottleneck. Moreover, you can't use this `Strategy`  object as a resident service.

## Converting to a Ray Serve Deployment

Through Ray Serve, you can deploy the core computing logic of `Strategy` as a scalable distributed computing service.

First, extract the indicator calculation of each institution into a separate `StrategyOnRayServe` class:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyOnRayServe.java
:end-before: docs-strategy-end
:language: java
:start-after: docs-strategy-start
```

Next, start the Ray Serve runtime and deploy `StrategyOnRayServe` as a deployment.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-deploy-end
:language: java
:start-after: docs-deploy-start
```

The `Deployment.create` makes a Deployment object named `strategy`. After executing `Deployment.deploy`, the Ray Serve instance deploys this `strategy` deployment with four replicas, and you can access it for distributed parallel computing.

## Testing the Ray Serve Deployment

You can test the `strategy` deployment using RayServeHandle inside Ray:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-calc-end
:language: java
:start-after: docs-calc-start
```

This code executes the calculation of each bank's each indicator serially, and sends it to Ray for execution. You can make the calculation concurrent, which not only improves the calculation efficiency, but also solves the bottleneck of single machine.

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-parallel-calc-end
:language: java
:start-after: docs-parallel-calc-start
```

You can use `StrategyCalcOnRayServe` like the example in the `main` method:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/StrategyCalcOnRayServe.java
:end-before: docs-main-end
:language: java
:start-after: docs-main-start
```

## Calling Ray Serve Deployment with HTTP

Another way to test or call a deployment is through the HTTP request. However, two limitations exist for the Java deployments:

- Only the `call` method of the user class can process the HTTP requests.

- The `call` method can only have one input parameter, and the type of the input parameter and the returned value can only be `String`.

If you want to call the `strategy` deployment with HTTP, then you can rewrite the class like this code:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyOnRayServe.java
:end-before: docs-strategy-end
:language: java
:start-after: docs-strategy-start
```

After deploying this deployment, you can access it with the `curl` command:

```shell
curl -d '{"time":1641038674, "bank":"test_bank", "indicator":"test_indicator"}' http://127.0.0.1:8000/strategy
```

You can also access it using HTTP Client in Java code:

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

You can also rewrite this code to support concurrency:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyCalcOnRayServe.java
:end-before: docs-parallel-calc-end
:language: java
:start-after: docs-parallel-calc-start
```

Finally, the complete usage of `HttpStrategyCalcOnRayServe` is like this code:

```{literalinclude} ../../../../java/serve/src/test/java/io/ray/serve/docdemo/HttpStrategyCalcOnRayServe.java
:end-before: docs-main-end
:language: java
:start-after: docs-main-start
```
