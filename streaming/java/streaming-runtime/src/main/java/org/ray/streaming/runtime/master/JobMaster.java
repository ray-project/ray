package org.ray.streaming.runtime.master;

import java.util.Map;
import org.ray.api.annotation.RayRemote;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.StreamingMasterConfig;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.util.ModuleNameAppender;
import org.ray.streaming.runtime.util.TestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RayRemote
public class JobMaster implements IJobMaster {

  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);

  private JobMasterRuntimeContext runtimeContext;
  private StreamingMasterConfig conf;
  private GraphManager graphManager;

  // For test
  public static JobMaster jobMaster;

  public JobMaster(Map<String, String> confMap) {
    if (TestHelper.isUTPattern()) {
      jobMaster = this;
    }
    LOG.info("Job master conf is {}.", confMap);

    StreamingConfig streamingConfig = new StreamingConfig(confMap);
    this.conf = streamingConfig.masterConfig;

    // TODO: init state backend

    // init runtime context
    runtimeContext = new JobMasterRuntimeContext(streamingConfig);

    String moduleName = conf.commonConfig.jobName();
    ModuleNameAppender.setModuleName(moduleName);

    LOG.info("Job master init success");
  }

  @Override
  public Boolean init(boolean isRecover) {
    LOG.info("Start to init job master. Is recover: {}.", isRecover);
    return true;
  }

  public JobMasterRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public GraphManager getGraphManager() {
    return graphManager;
  }

  public StreamingMasterConfig getConf() {
    return conf;
  }
}
