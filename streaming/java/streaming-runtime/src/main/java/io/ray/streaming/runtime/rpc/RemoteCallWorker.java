package io.ray.streaming.runtime.rpc;

import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ray call worker.
 * It takes the communication job from {@link JobMaster} to {@link JobWorker}.
 */
public class RemoteCallWorker {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  /**
   * Call JobWorker actor to init.
   *
   * @param actor target JobWorker actor
   * @param ctx JobWorker's context
   * @return init result
   */
  public static RayObject<Boolean> initWorker(RayActor actor, JobWorkerContext ctx) {
    LOG.info("Call worker to init, actor: {}, context: {}.", actor.getId(), ctx);
    RayObject<Boolean> result = null;

    // TODO (datayjz): ray call worker to initiate

    LOG.info("Finished calling worker to init.");
    return result;
  }

  /**
   * Call JobWorker actor to start.
   *
   * @param actor target JobWorker actor
   * @return start result
   */
  public static RayObject<Boolean> startWorker(RayActor actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    RayObject<Boolean> result = null;

    // TODO (datayjz): ray call worker to start

    LOG.info("Finished calling worker to start.");
    return result;
  }

  /**
   * Call JobWorker actor to destroy without reconstruction.
   *
   * @param actor target JobWorker actor
   * @return destroy result
   */
  public static Boolean shutdownWithoutReconstruction(RayActor actor) {
    LOG.info("Call worker to shutdown without reconstruction, actor is {}.",
        actor.getId());
    Boolean result = false;

    // TODO (datayjz): ray call worker to destroy

    LOG.info("Finished calling wk shutdownWithoutReconstruction, result is {}.", result);
    return result;
  }

}
