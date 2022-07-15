package io.ray.docdemo;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import io.ray.api.function.RayFunc0;
import io.ray.api.function.RayFunc1;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;

/**
 * This class contains demo code of the Ray core walkthrough doc
 * (https://docs.ray.io/en/master/walkthrough.html).
 *
 * <p>Please keep them in sync.
 */
public class WalkthroughDemo {

  public static class MyRayApp {

    // A regular Java static method.
    public static int myFunction() {
      return 1;
    }

    public static int slowFunction() throws InterruptedException {
      TimeUnit.SECONDS.sleep(10);
      return 1;
    }

    public static int functionWithAnArgument(int value) {
      return value + 1;
    }

    public static int overloadFunction() {
      return 1;
    }

    public static int overloadFunction(int x) {
      return x;
    }
  }

  public static void demoTasks() {
    // Invoke the above method as a Ray remote function.
    // This will immediately return an object ref (a future) and then create
    // a task that will be executed on a worker process.
    ObjectRef<Integer> res = Ray.task(MyRayApp::myFunction).remote();

    // The result can be retrieved with ``ObjectRef::get``.
    Assert.assertTrue(res.get() == 1);

    // Invocations of Ray remote functions happen in parallel.
    // All computation is performed in the background, driven by Ray's internal event loop.
    for (int i = 0; i < 4; i++) {
      // This doesn't block.
      Ray.task(MyRayApp::slowFunction).remote();
    }

    // Invoke overloaded functions.
    Assert.assertEquals(
        (int) Ray.task((RayFunc0<Integer>) MyRayApp::overloadFunction).remote().get(), 1);
    Assert.assertEquals(
        (int) Ray.task((RayFunc1<Integer, Integer>) MyRayApp::overloadFunction, 2).remote().get(),
        2);

    ObjectRef<Integer> objRef1 = Ray.task(MyRayApp::myFunction).remote();
    Assert.assertTrue(objRef1.get() == 1);

    // You can pass an `ObjectRef` as an argument to another Ray remote function.
    ObjectRef<Integer> objRef2 = Ray.task(MyRayApp::functionWithAnArgument, objRef1).remote();
    Assert.assertTrue(objRef2.get() == 2);

    // Specify required resources.
    Ray.task(MyRayApp::myFunction).setResource("CPU", 2.0).setResource("GPU", 4.0).remote();

    // Ray aslo supports fractional and custom resources.
    Ray.task(MyRayApp::myFunction).setResource("GPU", 0.5).setResource("Custom", 1.0).remote();
  }

  public static void demoObjects() {
    // Put an object in Ray's object store.
    int y = 1;
    ObjectRef<Integer> objectRef = Ray.put(y);
    // Get the value of one object ref.
    Assert.assertTrue(objectRef.get() == 1);

    // Get the values of multiple object refs in parallel.
    List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      objectRefs.add(Ray.put(i));
    }
    List<Integer> results = Ray.get(objectRefs);
    Assert.assertEquals(results, ImmutableList.of(0, 1, 2));

    WaitResult<Integer> waitResult = Ray.wait(objectRefs, /*num_returns=*/ 1, /*timeoutMs=*/ 1000);
    System.out.println(waitResult.getReady()); // List of ready objects.
    System.out.println(waitResult.getUnready()); // list of unready objects.
  }

  // A regular Java class.
  public static class Counter {

    private int value = 0;

    public int increment() {
      this.value += 1;
      return this.value;
    }
  }

  public static void demoActors() {
    // Create an actor from this class.
    // `Ray.actor` takes a factory method that can produce
    // a `Counter` object. Here, we pass `Counter`'s constructor
    // as the argument.
    ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

    // Specify required resources for an actor.
    Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();

    // Call the actor.
    ObjectRef<Integer> objectRef = counter.task(Counter::increment).remote();
    Assert.assertTrue(objectRef.get() == 1);

    // Create ten Counter actors.
    List<ActorHandle<Counter>> counters = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      counters.add(Ray.actor(Counter::new).remote());
    }

    // Increment each Counter once and get the results. These tasks all happen in
    // parallel.
    List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
    for (ActorHandle<Counter> counterActor : counters) {
      objectRefs.add(counterActor.task(Counter::increment).remote());
    }
    // prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    System.out.println(Ray.get(objectRefs));

    // Increment the first Counter five times. These tasks are executed serially
    // and share state.
    objectRefs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      objectRefs.add(counters.get(0).task(Counter::increment).remote());
    }
    // prints [2, 3, 4, 5, 6]
    System.out.println(Ray.get(objectRefs));
  }

  public static void main(String[] args) {
    // Start Ray runtime. If you're connecting to an existing cluster, you can set
    // the `-Dray.address=<cluster-address>` java system property.
    Ray.init();

    demoTasks();

    demoObjects();

    demoActors();
  }
}
