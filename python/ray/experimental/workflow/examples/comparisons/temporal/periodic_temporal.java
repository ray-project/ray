// https://github.com/temporalio/samples-java/blob/master/src/main/java/io/temporal/samples/hello/HelloCron.java
public class HelloActivity {
  static final String TASK_QUEUE = "HelloCron";
  static final String CRON_WORKFLOW_ID = "HelloCron";

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
    String greet(String greeting);
  }

  public static class GreetingWorkflowImpl implements GreetingWorkflow {
    private final GreetingActivities activities =
        Workflow.newActivityStub(
            GreetingActivities.class,
            ActivityOptions.newBuilder().setScheduleToCloseTimeout(Duration.ofSeconds(10)).build());

    @Override
    public String greet(String name) {
      activities.greet("Hello " + name + "!");
    }
  }

  // impl of the activities method
  static class GreetingActivitiesImpl implements GreetingActivities {
    @Override
    public String greet(String greeting) {
      System.out.println(
          "From " + Activity.getExecutionContext().getInfo().getWorkflowId() + ": " + greeting);
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

    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder()
            .setWorkflowId(CRON_WORKFLOW_ID)
            .setTaskQueue(TASK_QUEUE)
            .setCronSchedule("* * * * *")
            .setWorkflowExecutionTimeout(Duration.ofMinutes(10))
            .setWorkflowRunTimeout(Duration.ofMinutes(1))
            .build();

    // start workflow exec
    GreetingWorkflow workflow = client.newWorkflowStub(GreetingWorkflow.class, workflowOptions);
    // exec workflow
    try {
      WorkflowExecution execution = WorkflowClient.start(workflow::greet, "World");
      System.out.println("Started " + execution);
    } catch (WorkflowExecutionAlreadyStarted e) {
      System.out.println("Already running as " + e.getExecution());
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
