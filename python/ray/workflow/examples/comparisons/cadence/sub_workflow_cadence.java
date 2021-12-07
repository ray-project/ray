// https://github.com/uber/cadence-java-samples/blob/master/src/main/java/com/uber/cadence/samples/hello/HelloChild.java
public static class GreetingWorkflowImpl implements GreetingWorkflow {

  @Override
  public String getGreeting(String name) {
    // Workflows are stateful. So a new stub must be created for each new child.
    GreetingChild child = Workflow.newChildWorkflowStub(GreetingChild.class);

    // This is a non blocking call that returns immediately.
    // Use child.composeGreeting("Hello", name) to call synchronously.
    Promise<String> greeting = Async.function(child::composeGreeting, "Hello", name);
    // Do something else here.
    return greeting.get(); // blocks waiting for the child to complete.
  }

  // This example shows how parent workflow return right after starting a child workflow,
  // and let the child run itself.
  private String demoAsyncChildRun(String name) {
    GreetingChild child = Workflow.newChildWorkflowStub(GreetingChild.class);
    // non blocking call that initiated child workflow
    Async.function(child::composeGreeting, "Hello", name);
    // instead of using greeting.get() to block till child complete,
    // sometimes we just want to return parent immediately and keep child running
    Promise<WorkflowExecution> childPromise = Workflow.getWorkflowExecution(child);
    childPromise.get(); // block until child started,
    // otherwise child may not start because parent complete first.
    return "let child run, parent just return";
  }

  public static void main(String[] args) {
    // Start a worker that hosts both parent and child workflow implementations.
    Worker.Factory factory = new Worker.Factory(DOMAIN);
    Worker worker = factory.newWorker(TASK_LIST);
    worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class, GreetingChildImpl.class);
    // Start listening to the workflow task list.
    factory.start();

    // Start a workflow execution. Usually this is done from another program.
    WorkflowClient workflowClient = WorkflowClient.newInstance(DOMAIN);
    // Get a workflow stub using the same task list the worker uses.
    GreetingWorkflow workflow = workflowClient.newWorkflowStub(GreetingWorkflow.class);
    // Execute a workflow waiting for it to complete.
    String greeting = workflow.getGreeting("World");
    System.out.println(greeting);
    System.exit(0);
  }
}
