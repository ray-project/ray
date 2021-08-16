// https://github.com/temporalio/samples-java/blob/master/src/main/java/io/temporal/samples/hello/HelloActivityRetry.java
public class HelloActivityRetry {
    static final String TASK_QUEUE = "HelloActivityRetry";

   // workflow interface
   @WorkflowInterface
   public interface GreetingWorkflow {
    @WorkflowMethod
        String getGreeting(String name);
    }
    // activity interface
    @ActivityInterface
    public interface GreetingActivities {
        @ActivityMethod
        String composeGreeting(String greeting, String name);
    }

    public static class GreetingWorkflowImpl implements GreetingWorkflow {
        private final GreetingActivities activities =
            Workflow.newActivityStub(
                GreetingActivities.class,
                ActivityOptions.newBuilder()
                    .setScheduleToCloseTimeout(Duration.ofSeconds(10))
                    .setRetryOptions(
                        RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setDoNotRetry(IllegalArgumentException.class.getName())
                            .build())
                    .build());
        @Override
        public String getGreeting(String name) {
          // This is a blocking call that returns only after activity is completed.
          return activities.composeGreeting("Hello", name);
        }
    }

    static class GreetingActivitiesImpl implements GreetingActivities {
    private int callCount;
    private long lastInvocationTime;

    @Override
    public synchronized String composeGreeting(String greeting, String name) {
      if (lastInvocationTime != 0) {
        long timeSinceLastInvocation = System.currentTimeMillis() - lastInvocationTime;
        System.out.print(timeSinceLastInvocation + " milliseconds since last invocation. ");
      }
      lastInvocationTime = System.currentTimeMillis();
      if (++callCount < 4) {
        System.out.println("composeGreeting activity is going to fail");
        throw new IllegalStateException("not yet");
      }
      System.out.println("composeGreeting activity is going to complete");
      return greeting + " " + name + "!";
    }
  }

    // main method
    public static void main(String[] args) {
       WorkflowServiceStubs service = WorkflowServiceStubs.newInstance();
       WorkflowClient client = WorkflowClient.newInstance(service);
       WorkerFactory factory = WorkerFactory.newInstance(client);
       Worker worker = factory.newWorker(TASK_QUEUE);

       WorkflowOptions workflowOptions = WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build();
       GreetingWorkflow workflow = client.newWorkflowStub(GreetingWorkflow.class, workflowOptions);

       // Execute a workflow waiting for it to complete.
       String greeting = workflow.getGreeting("World");
       System.out.println(greeting);
       System.exit(0);
    }
}
