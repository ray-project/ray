// https://github.com/uber/cadence-java-samples/tree/master/src/main/java/com/uber/cadence/samples/fileprocessing
public class FileProcessingWorkflowImpl implements FileProcessingWorkflow {

  // Uses the default task list shared by the pool of workers.
  private final StoreActivities defaultTaskListStore;

  public FileProcessingWorkflowImpl() {
    // Create activity clients.
    ActivityOptions ao =
        new ActivityOptions.Builder()
            .setScheduleToCloseTimeout(Duration.ofSeconds(10))
            .setTaskList(FileProcessingWorker.TASK_LIST)
            .build();
    this.defaultTaskListStore = Workflow.newActivityStub(StoreActivities.class, ao);
  }

  @Override
  public void processFile(URL source, URL destination) {
    RetryOptions retryOptions =
        new RetryOptions.Builder()
            .setExpiration(Duration.ofSeconds(10))
            .setInitialInterval(Duration.ofSeconds(1))
            .build();
    // Retries the whole sequence on any failure, potentially on a different host.
    Workflow.retry(retryOptions, () -> processFileImpl(source, destination));
  }

  private void processFileImpl(URL source, URL destination) {
    StoreActivities.TaskListFileNamePair downloaded = defaultTaskListStore.download(source);

    // Now initialize stubs that are specific to the returned task list.
    ActivityOptions hostActivityOptions =
        new ActivityOptions.Builder()
            .setTaskList(downloaded.getHostTaskList())
            .setScheduleToCloseTimeout(Duration.ofSeconds(10))
            .build();
    StoreActivities hostSpecificStore =
        Workflow.newActivityStub(StoreActivities.class, hostActivityOptions);

    // Call processFile activity to zip the file.
    // Call the activity to process the file using worker-specific task list.
    String processed = hostSpecificStore.process(downloaded.getFileName());
    // Call upload activity to upload the zipped file.
    hostSpecificStore.upload(processed, destination);
  }
}
