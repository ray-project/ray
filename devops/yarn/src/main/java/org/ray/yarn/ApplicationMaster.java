package org.ray.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.sun.jersey.api.client.ClientHandlerException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.log4j.LogManager;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  @VisibleForTesting
  @Private
  public static enum DsEvent {
    DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
  }

  @VisibleForTesting
  @Private
  public static enum DsEntity {
    DS_APP_ATTEMPT, DS_CONTAINER
  }

  private static final String YARN_SHELL_ID = "YARN_SHELL_ID";
  // Configuration
  private Configuration conf;
  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRmClient;
  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;
  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NmCallbackHandler containerListener;
  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  protected ApplicationAttemptId appAttemptId;
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";
  // App Master configuration
  // No. of containers to run shell command on
  @VisibleForTesting
  protected int numTotalContainers = 1;
  // Memory to request for the container on which the shell command will run
  private long containerMemory = 10;
  // VirtualCores to request for the container on which the shell command will run
  private int containerVirtualCores = 1;
  // Priority of the request
  private int requestPriority;
  // No. of each of the Ray roles including head and work
  private Map<String, Integer> numRoles = Maps.newHashMapWithExpectedSize(2);
  private RayNodeContext[] indexToNode = null;
  private Map<String, RayNodeContext> containerToNode = Maps.newHashMap();

  // The default value should be consistent with Ray RunParameters
  private int redisPort = 34222;
  private String redisAddress;

  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  protected AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  protected AtomicInteger numRequestedContainers = new AtomicInteger();

  // Shell command to be executed
  private String shellCommand = "";
  // Args to be passed to the shell command
  private String shellArgs = "";
  // Env variables to be setup for the shell command
  private Map<String, String> shellEnv = new HashMap<String, String>();

  // Location of shell script ( obtained from info set in env )
  // Shell script path in fs
  private String rayArchiveFile = "";
  // Timestamp needed for creating a local resource
  private long rayArchiveFileTimestamp = 0;
  // File length needed for local resource
  private long rayArchiveFileLen = 0;

  // Timeline domain ID
  private String domainId = null;

  // Hardcoded path to shell script in launch container's local env
  private static final String rayShellStringPath = "run.sh";
  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";
  private static final String shellCommandPath = "shellCommands";
  private static final String shellArgsPath = "shellArgs";

  private volatile boolean done;

  private ByteBuffer allTokens;

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();

  // Timeline Client
  @VisibleForTesting
  TimelineClient timelineClient;
  static final String CONTAINER_ENTITY_GROUP_ID = "CONTAINERS";
  static final String APPID_TIMELINE_FILTER_NAME = "appId";
  static final String USER_TIMELINE_FILTER_NAME = "user";
  static final String LINUX_BASH_COMMEND = "bash";

  private int rayInstanceCounter = 1;

  // the static args of head node
  private String headNodeStaticArgs = null;
  //the static args of head node
  private String workNodeStaticArgs = null;
  // supremeFo flag
  private boolean supremeFo = false;
  // disable process failover flag
  private boolean disableProcessFo = true;

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers =
      Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  /**
   * The main entrance of appMaster.
   * @param args Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging.
   */
  private void dumpOutDebugInfo() {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
    }

    BufferedReader buf = null;
    try {
      String lines = Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanup(LOG, buf);
    }
  }

  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
  }

  /**
   * Parse command line options.
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   */
  public boolean init(String[] args) throws ParseException, IOException {
    Options opts = new Options();
    opts.addOption("appAttemptId", true,
        "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("shellEnv", true,
        "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption("containerMemory", true,
        "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("containerVcores", true,
        "Amount of virtual cores to be requested to run the shell command");
    opts.addOption("numContainers", true,
        "No. of containers on which the shell command needs to be executed");
    opts.addOption("numRoles", true,
        "No. of the Ray roles including head and work");
    opts.getOption("numRoles").setArgs(2);
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("headNodeStaticArgs", true,
        "the static args which is needed when start head node");
    opts.addOption("workNodeStaticArgs", true,
        "the static args which is needed when start work node");
    opts.addOption("supremeFo", false, "use supreme failover strategy");
    opts.addOption("disableProcessFo", false, "disable process failover");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for application master to initialize");
    }

    // Check whether customer log4j.properties file exists
    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class, log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    if (cliParser.hasOption("headNodeStaticArgs")) {
      headNodeStaticArgs = cliParser.getOptionValue("headNodeStaticArgs");
    }

    if (cliParser.hasOption("workNodeStaticArgs")) {
      workNodeStaticArgs = cliParser.getOptionValue("workNodeStaticArgs");
    }

    if (cliParser.hasOption("supremeFo")) {
      supremeFo = true;
    }

    if (cliParser.hasOption("disableProcessFo")) {
      disableProcessFo = true;
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("appAttemptId")) {
        String appIdStr = cliParser.getOptionValue("appAttemptId", "");
        appAttemptId = ApplicationAttemptId.fromString(appIdStr);
      } else {
        throw new IllegalArgumentException("Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ContainerId.fromString(envs.get(Environment.CONTAINER_ID.name()));
      appAttemptId = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(
          ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
    }

    LOG.info("Application master for app" + ", appId=" + appAttemptId.getApplicationId().getId()
        + ", clustertimestamp=" + appAttemptId.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptId.getAttemptId());

    if (!fileExist(shellCommandPath) && envs.get(DsConstants.RAY_ARCHIVE_LOCATION).isEmpty()) {
      throw new IllegalArgumentException(
          "No shell command or shell script specified to be executed by application master");
    }

    if (fileExist(shellCommandPath)) {
      shellCommand = readContent(shellCommandPath);
    }

    if (fileExist(shellArgsPath)) {
      shellArgs = readContent(shellArgsPath);
    }

    if (cliParser.hasOption("shellEnv")) {
      String[] shellEnvs = cliParser.getOptionValues("shellEnv");
      for (String env : shellEnvs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          shellEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        shellEnv.put(key, val);
      }
    }

    if (envs.containsKey(DsConstants.RAY_ARCHIVE_LOCATION)) {
      rayArchiveFile = envs.get(DsConstants.RAY_ARCHIVE_LOCATION);

      if (envs.containsKey(DsConstants.RAY_ARCHIVE_TIMESTAMP)) {
        rayArchiveFileTimestamp = Long.parseLong(envs.get(DsConstants.RAY_ARCHIVE_TIMESTAMP));
      }
      if (envs.containsKey(DsConstants.RAY_ARCHIVE_LEN)) {
        rayArchiveFileLen = Long.parseLong(envs.get(DsConstants.RAY_ARCHIVE_LEN));
      }
      if (!rayArchiveFile.isEmpty() && (rayArchiveFileTimestamp <= 0 || rayArchiveFileLen <= 0)) {
        LOG.error("Illegal values in env for shell script path" + ", path=" + rayArchiveFile
            + ", len=" + rayArchiveFileLen + ", timestamp=" + rayArchiveFileTimestamp);
        throw new IllegalArgumentException("Illegal values in env for shell script path");
      }
    }

    if (envs.containsKey(DsConstants.RAY_TIMELINE_DOMAIN)) {
      domainId = envs.get(DsConstants.RAY_TIMELINE_DOMAIN);
    }

    containerMemory = Integer.parseInt(cliParser.getOptionValue("containerMemory", "10"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("containerVcores", "1"));
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("numContainers", "1"));
    if (numTotalContainers == 0) {
      throw new IllegalArgumentException("Cannot run distributed shell with no containers");
    }
    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

    if (cliParser.hasOption("numRoles")) {
      String[] optNumRoles = cliParser.getOptionValues("numRoles");
      numRoles.put("head", Integer.parseInt(optNumRoles[0]));
      numRoles.put("work", Integer.parseInt(optNumRoles[1]));
      // Argument check has been done in Client
    } else {
      numRoles.put("head", 1);
      numRoles.put("work", 0);
    }

    numTotalContainers = numRoles.get("head") + numRoles.get("work");

    indexToNode = new RayNodeContext[numTotalContainers];
    int i = 0;
    if (numRoles.get("head") == 1) {
      indexToNode[i] = new RayNodeContext("head");
      ++i;
    }
    for (int j = 0; j < numRoles.get("work"); ++j) {
      indexToNode[i] = new RayNodeContext("work");
      ++i;
    }
    assert numTotalContainers == i;
    return true;
  }

  /**
   * Helper function to print usage.
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  @SuppressWarnings("unchecked")
  private int setupContainerRequest() {
    int requestCount = 0;
    for (RayNodeContext nodeContext : indexToNode) {
      if (nodeContext.isRunning == false && nodeContext.isAlocating == false) {
        ContainerRequest containerAsk = setupContainerAskForRm();
        amRmClient.addContainerRequest(containerAsk);
        requestCount++;
        nodeContext.isAlocating = true;
        LOG.info("Setup container request: " + containerAsk);
      }
    }
    LOG.info("Setup container request, count is " + requestCount);
    return requestCount;
  }

  /**
   * Main run function for the application master.
   */
  public void run() throws YarnException, IOException, InterruptedException {
    LOG.info("Starting ApplicationMaster");

    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
    appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);

    AMRMClientAsync.AbstractCallbackHandler allocListener = new RmCallbackHandler();
    amRmClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRmClient.init(conf);
    amRmClient.start();

    containerListener = createNmCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    // startTimelineClient(conf);
    if (timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptId.toString(),
          DsEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);
    }

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRmClient
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);
    // Dump out information about cluster capability as seen by the
    // resource manager
    long maxMem = response.getMaximumResourceCapability().getMemorySize();
    LOG.info("Max mem capability of resources in this cluster " + maxMem);

    int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

    // A resource ask cannot exceed the max.
    if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster." + " Using max value."
          + ", specified=" + containerMemory + ", max=" + maxMem);
      containerMemory = maxMem;
    }

    if (containerVirtualCores > maxVCores) {
      LOG.info("Container virtual cores specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerVirtualCores + ", max=" + maxVCores);
      containerVirtualCores = maxVCores;
    }

    List<Container> previousAmRunningContainers = response.getContainersFromPreviousAttempts();
    LOG.info(appAttemptId + " received " + previousAmRunningContainers.size()
        + " previous attempts' running containers on AM registration.");
    for (Container container : previousAmRunningContainers) {
      launchedContainers.add(container.getId());
    }
    numAllocatedContainers.addAndGet(previousAmRunningContainers.size());

    int numTotalContainersToRequest = numTotalContainers - previousAmRunningContainers.size();

    if (previousAmRunningContainers.size() > 0) {
      // TODO: support failover about recovery ray node context
      LOG.warn("Some previous containers found.");
    }
    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    int requestCount = setupContainerRequest();

    assert requestCount == numTotalContainersToRequest : "The request count is inconsistent: "
        + requestCount + " != " + numTotalContainersToRequest;

    // for (int i = 0; i < numTotalContainersToRequest; ++i) {
    // ContainerRequest containerAsk = setupContainerAskForRM(null);
    // amRMClient.addContainerRequest(containerAsk);
    // }
    numRequestedContainers.set(numTotalContainers);
  }

  @VisibleForTesting
  void startTimelineClient(final Configuration conf)
      throws YarnException, IOException, InterruptedException {
    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            // Creating the Timeline Client
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(conf);
            timelineClient.start();
          } else {
            timelineClient = null;
            LOG.warn("Timeline service is not enabled");
          }
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new YarnException(e.getCause());
    }
  }

  @VisibleForTesting
  NmCallbackHandler createNmCallbackHandler() {
    return new NmCallbackHandler(this);
  }

  @VisibleForTesting
  protected boolean finish() {
    // wait for completion.
    while (!done && (numCompletedContainers.get() != numTotalContainers)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
        LOG.warn("Catch InterruptedException when sleep.");
      }
    }

    if (timelineClient != null) {
      publishApplicationAttemptEvent(timelineClient, appAttemptId.toString(),
          DsEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
    }

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed="
          + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get()
          + ", failed=" + numFailedContainers.get();
      LOG.info(appMessage);
      success = false;
    }
    try {
      amRmClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRmClient.stop();

    // Stop Timeline Client
    if (timelineClient != null) {
      timelineClient.stop();
    }

    return success;
  }

  @VisibleForTesting
  class RmCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      Boolean restartClasterFlag = false;
      LOG.info(
          "Got response from RM for container ask, completedCnt=" + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info(appAttemptId + " got container status for containerID="
            + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
            + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        // ignore containers we know nothing about - probably from a previous
        // attempt
        if (!launchedContainers.contains(containerStatus.getContainerId())) {
          LOG.info("Ignoring completed status of " + containerStatus.getContainerId()
              + "; unknown container(probably launched by previous attempt)");
          continue;
        }

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          LOG.info("container failed, exit status is " + exitStatus);
          for (RayNodeContext node : indexToNode) {
            if (node.container != null
                && node.container.getId().equals(containerStatus.getContainerId())) {
              LOG.info("ray node failed, the role is " + node.role);
              if (-100 == exitStatus) { /* release container will return -100 */
                if (node.isRunning == false) {
                  LOG.info("release container will return -100, don't process it");
                  break;
                } else {
                  LOG.warn("the exit status is -100, but this node should be running");
                }
              }
              node.isRunning = false;
              node.isAlocating = false;
              node.instanceId = null;
              node.container = null;
              node.failCounter++;

              if (disableProcessFo) {
                LOG.info("process failover is disable, ignore container failed");
                break;
              }

              if (supremeFo) {
                LOG.info("Start supreme failover");
                restartClasterFlag = true;
              }

              if (node.role == "head") {
                restartClasterFlag = true;
              }
              numAllocatedContainers.decrementAndGet();
              numRequestedContainers.decrementAndGet();
              break;
            }
          }

          if (restartClasterFlag) {
            LOG.info("restart all the Container of ray node");
            for (RayNodeContext node : indexToNode) {
              if (node.isRunning && node.container != null) {
                amRmClient.releaseAssignedContainer(node.container.getId());
                node.isRunning = false;
                node.isAlocating = false;
                node.instanceId = null;
                node.container = null;
                node.failCounter++;
                numAllocatedContainers.decrementAndGet();
                numRequestedContainers.decrementAndGet();
              }
            }
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
        if (timelineClient != null) {
          publishContainerEndEvent(timelineClient, containerStatus, domainId, appSubmitterUgi);
        }

        if (restartClasterFlag) {
          break;
        }
      }

      // ask for more containers if any failed
      int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      int requestCount = setupContainerRequest();
      assert requestCount == askCount : "The request count is inconsistent(onContainersCompleted): "
          + requestCount + " != " + askCount;

      if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info(
          "Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        String rayInstanceId = Integer.toString(rayInstanceCounter);
        rayInstanceCounter++;

        Thread launchThread = null;
        boolean shouldSleep = false;
        for (RayNodeContext node : indexToNode) {
          if (node.isRunning) {
            continue;
          }
          node.isRunning = true;
          node.isAlocating = false;
          node.instanceId = rayInstanceId;
          node.container = allocatedContainer;
          containerToNode.put(allocatedContainer.getId().toString(), node);
          if (node.role == "head") {
            try {
              redisAddress =
                  InetAddress.getByName(allocatedContainer.getNodeHttpAddress().split(":")[0])
                      .getHostAddress() + ":" + redisPort;
            } catch (UnknownHostException e) {
              redisAddress = "";
            }
          } else {
            shouldSleep = true;
          }
          launchThread = createLaunchContainerThread(allocatedContainer, rayInstanceId, node.role,
              shouldSleep ? 20000 : 0);
          break;
        }

        if (launchThread == null) {
          LOG.error("The container " + allocatedContainer + " unused!");
          break;
        }

        LOG.info("Launching Ray instance on a new container." + ", containerId="
            + allocatedContainer.getId() + ", rayInstanceId=" + rayInstanceId + ", containerNode="
            + allocatedContainer.getNodeId().getHost() + ":"
            + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
            + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory="
            + allocatedContainer.getResource().getMemorySize() + ", containerResourceVirtualCores="
            + allocatedContainer.getResource().getVirtualCores());
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());

        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchedContainers.add(allocatedContainer.getId());
        launchThread.start();
      }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {}

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) numCompletedContainers.get() / numTotalContainers;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      LOG.error("Error in RMCallbackHandler: ", e);
      done = true;
      amRmClient.stop();
    }
  }

  @VisibleForTesting
  static class NmCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

    private ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;

    public NmCallbackHandler(ApplicationMaster applicationMaster) {
      this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" + containerStatus);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
      }
      if (applicationMaster.timelineClient != null) {
        applicationMaster.publishContainerStartEvent(applicationMaster.timelineClient, container,
            applicationMaster.domainId, applicationMaster.appSubmitterUgi);
      }
    }

    @Override
    public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {}

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
      applicationMaster.numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }

    @Override
    public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {}

  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container that will
   * execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    private Container container;
    private String rayInstanceId;
    private String role;
    private long sleepMillis = 0;

    NmCallbackHandler containerListener;

    public LaunchContainerRunnable(Container lcontainer, NmCallbackHandler containerListener,
        String rayInstanceId, String role, long sleepMillis) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.rayInstanceId = rayInstanceId;
      this.role = role;
      this.sleepMillis = sleepMillis;
    }

    @Override
    /**
     * Connects to CM, sets up container launch context for shell command and eventually dispatches
     * the container start request to the CM.
     */
    public void run() {
      LOG.info("Setting up container launch container for containerid=" + container.getId()
          + " with rayInstanceId=" + rayInstanceId + " ,sleep millis " + sleepMillis);

      if (sleepMillis != 0) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
          LOG.warn("Catch InterruptedException when sleep.");
        }

      }
      // Set the local resources
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      // The container for the eventual shell commands needs its own local
      // resources too.
      // In this scenario, if a shell script is specified, we need to have it
      // copied and made available to the container.
      String rayArchiveFileLocalPath = "Ray-package";
      if (!rayArchiveFile.isEmpty()) {
        Path rayArchivePath = new Path(rayArchiveFile);

        URL yarnUrl = null;
        try {
          yarnUrl = URL.fromURI(new URI(rayArchivePath.toString()));
        } catch (URISyntaxException e) {
          LOG.error("Error when trying to use Ray archive path specified" + " in env, path="
              + rayArchivePath, e);
          // A failure scenario on bad input such as invalid shell script path
          // We know we cannot continue launching the container
          // so we should release it.
          // TODO
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          return;
        }
        LocalResource rayRsrc = LocalResource.newInstance(yarnUrl, LocalResourceType.ARCHIVE,
            LocalResourceVisibility.APPLICATION, rayArchiveFileLen, rayArchiveFileTimestamp);
        localResources.put(rayArchiveFileLocalPath, rayRsrc);
        shellCommand = LINUX_BASH_COMMEND;
      }

      // Set the necessary command to execute on the allocated container
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);

      // Set executable command
      vargs.add(shellCommand);
      // Set shell script path
      if (!rayArchiveFile.isEmpty()) {
        vargs.add(rayArchiveFileLocalPath + "/" + rayShellStringPath);
      }

      // Set args based on role
      switch (role) {
        case "head":
          vargs.add("--head");
          vargs.add("--redis-address");
          vargs.add(redisAddress);
          if (headNodeStaticArgs != null) {
            vargs.add(headNodeStaticArgs);
          }
          break;
        case "work":
          //vargs.add("--work");
          vargs.add("--redis_port");
          vargs.add(String.valueOf(redisPort));
          if (workNodeStaticArgs != null) {
            vargs.add(workNodeStaticArgs);
          }
          break;
        default:
          break;
      }

      try {
        String nodeIpAddress =
            InetAddress.getByName(container.getNodeHttpAddress().split(":")[0]).getHostAddress();
        vargs.add("--node-ip-address");
        vargs.add(nodeIpAddress);
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }

      // Set args for the shell command if any
      vargs.add(shellArgs);

      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());
      LOG.info("command: " + commands);

      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.

      Map<String, String> myShellEnv = new HashMap<String, String>(shellEnv);
      myShellEnv.put(YARN_SHELL_ID, rayInstanceId);
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, myShellEnv,
          commands, null, allTokens.duplicate(), null);
      containerListener.addContainer(container.getId(), container);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRm() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(requestPriority);

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }

  private String readContent(String filePath) throws IOException {
    DataInputStream ds = null;
    try {
      ds = new DataInputStream(new FileInputStream(filePath));
      return ds.readUTF();
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ds);
    }
  }

  private void publishContainerStartEvent(final TimelineClient timelineClient,
      final Container container, String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getId().toString());
    entity.setEntityType(DsEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME,
        container.getId().getApplicationAttemptId().getApplicationId().toString());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(DsEvent.DS_CONTAINER_START.toString());
    event.addEventInfo("Node", container.getNodeId().toString());
    event.addEventInfo("Resources", container.getResource().toString());
    entity.addEvent(event);

    try {
      processTimelineResponseErrors(
          putContainerEntity(timelineClient, container.getId().getApplicationAttemptId(), entity));
    } catch (YarnException | IOException | ClientHandlerException e) {
      LOG.error("Container start event could not be published for " + container.getId().toString(),
          e);
    }
  }

  @VisibleForTesting
  void publishContainerEndEvent(final TimelineClient timelineClient, ContainerStatus container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getContainerId().toString());
    entity.setEntityType(DsEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME,
        container.getContainerId().getApplicationAttemptId().getApplicationId().toString());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(DsEvent.DS_CONTAINER_END.toString());
    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    try {
      processTimelineResponseErrors(putContainerEntity(timelineClient,
          container.getContainerId().getApplicationAttemptId(), entity));
    } catch (YarnException | IOException | ClientHandlerException e) {
      LOG.error(
          "Container end event could not be published for " + container.getContainerId().toString(),
          e);
    }
  }

  private TimelinePutResponse putContainerEntity(TimelineClient timelineClient,
      ApplicationAttemptId currAttemptId, TimelineEntity entity) throws YarnException, IOException {
    if (TimelineUtils.timelineServiceV1_5Enabled(conf)) {
      TimelineEntityGroupId groupId = TimelineEntityGroupId
          .newInstance(currAttemptId.getApplicationId(), CONTAINER_ENTITY_GROUP_ID);
      return timelineClient.putEntities(currAttemptId, groupId, entity);
    } else {
      return timelineClient.putEntities(entity);
    }
  }

  private void publishApplicationAttemptEvent(final TimelineClient timelineClient,
      String appAttemptId, DsEvent appEvent, String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(appAttemptId);
    entity.setEntityType(DsEntity.DS_APP_ATTEMPT.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);
    try {
      TimelinePutResponse response = timelineClient.putEntities(entity);
      processTimelineResponseErrors(response);
    } catch (YarnException | IOException | ClientHandlerException e) {
      LOG.error("App Attempt " + (appEvent.equals(DsEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for " + appAttemptId.toString(), e);
    }
  }

  private TimelinePutResponse processTimelineResponseErrors(TimelinePutResponse response) {
    List<TimelinePutResponse.TimelinePutError> errors = response.getErrors();
    if (errors.size() == 0) {
      LOG.debug("Timeline entities are successfully put");
    } else {
      for (TimelinePutResponse.TimelinePutError error : errors) {
        LOG.error("Error when publishing entity [" + error.getEntityType() + ","
            + error.getEntityId() + "], server side error code: " + error.getErrorCode());
      }
    }
    return response;
  }

  RmCallbackHandler getRmCallbackHandler() {
    return new RmCallbackHandler();
  }

  @SuppressWarnings("rawtypes")
  @VisibleForTesting
  void setAmRmClient(AMRMClientAsync client) {
    this.amRmClient = client;
  }

  @VisibleForTesting
  int getNumCompletedContainers() {
    return numCompletedContainers.get();
  }

  @VisibleForTesting
  boolean getDone() {
    return done;
  }

  @VisibleForTesting
  Thread createLaunchContainerThread(Container allocatedContainer, String shellId, String role,
      long sleepMillis) {
    LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
        allocatedContainer, containerListener, shellId, role, sleepMillis);
    return new Thread(runnableLaunchContainer);
  }
}
