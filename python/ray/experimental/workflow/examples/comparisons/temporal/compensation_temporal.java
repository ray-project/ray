// https://github.com/temporalio/samples-java/blob/8218f4114e52417f8d04175b67027ff0af4fb73c/src/main/java/io/temporal/samples/hello/HelloSaga.java
public class HelloActivity {
    static final String TASK_QUEUE = "HelloActivity";

   // workflow interface
   @WorkflowInterface
   public interface ChildWorkflowOperation {
     @WorkflowMethod
     void execute(int amount);
   }

   public static class ChildWorkflowOperationImpl implements ChildWorkflowOperation {
       ActivityOperation activity =
           Workflow.newActivityStub(
               ActivityOperation.class,
               ActivityOptions.newBuilder().setScheduleToCloseTimeout(Duration.ofSeconds(10)).build());

       @Override
       public void execute(int amount) {
         activity.execute(amount);
       }
   }

    @WorkflowInterface
    public interface ChildWorkflowCompensation {
      @WorkflowMethod
      void compensate(int amount);
    }

    public static class ChildWorkflowCompensationImpl implements ChildWorkflowCompensation {
     ActivityOperation activity =
         Workflow.newActivityStub(
             ActivityOperation.class,
             ActivityOptions.newBuilder().setScheduleToCloseTimeout(Duration.ofSeconds(10)).build());

     @Override
     public void compensate(int amount) {
       activity.compensate(amount);
     }
    }

    @ActivityInterface
    public interface ActivityOperation {
      @ActivityMethod
      void execute(int amount);
      @ActivityMethod
      void compensate(int amount);
   }

    public static class ActivityOperationImpl implements ActivityOperation {
        @Override
        public void execute(int amount) {
          System.out.println("ActivityOperationImpl.execute() is called with amount " + amount);
        }

        @Override
        public void compensate(int amount) {
          System.out.println("ActivityCompensationImpl.compensate() is called with amount " + amount);
        }
      }

      @WorkflowInterface
      public interface SagaWorkflow {
        @WorkflowMethod
        void execute();
    }

    public static class SagaWorkflowImpl implements SagaWorkflow {
    ActivityOperation activity =
            Workflow.newActivityStub(
                ActivityOperation.class,
                ActivityOptions.newBuilder().setScheduleToCloseTimeout(Duration.ofSeconds(2)).build());

        @Override
        public void execute() {
          Saga saga = new Saga(new Saga.Options.Builder().setParallelCompensation(false).build());
          try {
            // The following demonstrate how to compensate sync invocations.
            ChildWorkflowOperation op1 = Workflow.newChildWorkflowStub(ChildWorkflowOperation.class);
            op1.execute(10);
            ChildWorkflowCompensation c1 =
                Workflow.newChildWorkflowStub(ChildWorkflowCompensation.class);
            saga.addCompensation(c1::compensate, -10);

            // The following demonstrate how to compensate async invocations.
            Promise<Void> result = Async.procedure(activity::execute, 20);
            saga.addCompensation(activity::compensate, -20);
            result.get();

            saga.addCompensation(
                () -> System.out.println("Other compensation logic in main workflow."));
            throw new RuntimeException("some error");

          } catch (Exception e) {
            saga.compensate();
          }
        }
    }

    // main method
    public static void main(String[] args) {
       WorkflowServiceStubs service = WorkflowServiceStubs.newInstance();
       WorkflowClient client = WorkflowClient.newInstance(service);
       WorkerFactory factory = WorkerFactory.newInstance(client);
       Worker worker = factory.newWorker(TASK_QUEUE);
       worker.registerWorkflowImplementationTypes(
           HelloSaga.SagaWorkflowImpl.class,
           HelloSaga.ChildWorkflowOperationImpl.class,
           HelloSaga.ChildWorkflowCompensationImpl.class);
       worker.registerActivitiesImplementations(new ActivityOperationImpl());
       factory.start();

       // start workflow exec
       WorkflowOptions workflowOptions = WorkflowOptions.newBuilder().setTaskQueue(TASK_QUEUE).build();
       HelloSaga.SagaWorkflow workflow =
           client.newWorkflowStub(HelloSaga.SagaWorkflow.class, workflowOptions);
       workflow.execute();
       System.exit(0);

    }
}
