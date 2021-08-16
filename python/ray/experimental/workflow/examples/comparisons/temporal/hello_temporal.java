// https://github.com/temporalio/samples-java/blob/master/src/main/java/io/temporal/samples/hello/HelloActivity.java
public class HelloActivity {
    static final String TASK_QUEUE = "HelloActivity";

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
                ActivityOptions.newBuilder().setScheduleToCloseTimeout(Duration.ofSeconds(2)).build());

        // workflow "getGreting" method that calls the activities methods
        @Override
        public String getGreeting(String name) {
          return activities.composeGreeting("Hello", name);
        }
    }

    // impl of the activities method
    static class GreetingActivitiesImpl implements GreetingActivities {
      @Override
      public String composeGreeting(String greeting, String name) {
        return greeting + " " + name + "!";
      }
    }

    // main method
    public static void main(String[] args) {
       WorkflowServiceStubs service = WorkflowServiceStubs.newInstance();
       WorkflowClient client = WorkflowClient.newInstance(service);
       WorkerFactory factory = WorkerFactory.newInstance(client);
       Worker worker = factory.newWorker(TASK_QUEUE);

       // create workflow instance
       worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
       // register activity
       worker.registerActivitiesImplementations(new GreetingActivitiesImpl());
       factory.start();

       // start workflow exec
       GreetingWorkflow workflow =
        client.newWorkflowStub(
          GreetingWorkflow.class,
          WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build());
       // exec workflow
       String greeting = workflow.getGreeting("World");

    }
}
