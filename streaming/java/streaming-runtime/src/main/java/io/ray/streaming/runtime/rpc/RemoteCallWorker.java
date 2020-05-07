package io.ray.streaming.runtime.rpc;

import io.ray.api.BaseActor;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.RayPyActor;
import io.ray.api.function.PyActorMethod;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JavaJobWorkerContext;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import io.ray.streaming.runtime.worker.context.PythonJobWorkerContext;
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
   * @param context JobWorker's context
   * @return init result
   */
  public static RayObject<Boolean> initWorker(BaseActor actor, JobWorkerContext context) {
    LOG.info("Call worker to initiate, actor: {}, context: {}.", actor.getId(), context);
    RayObject<Boolean> result;

    // python
    if (actor instanceof RayPyActor) {
      result = ((RayPyActor) actor).call(
          new PyActorMethod("init", Object.class), context.getContextBytes());
    } else {
      // java
      result = ((RayActor<JobWorker>) actor).call(JobWorker::init, (JavaJobWorkerContext) context);
    }

    LOG.info("Finished calling worker to initiate.");
    return result;
  }

  /**
   * Call JobWorker actor to start.
   *
   * @param actor target JobWorker actor
   * @return start result
   */
  public static RayObject<Boolean> startWorker(BaseActor actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // result = Ray.callPy((RayPyActor) actor, "start");
    } else {
      // java
      result = ((RayActor<JobWorker>) actor).call(JobWorker::start);
    }

    LOG.info("Finished calling worker to start.");
    return result;
  }

  /**
   * Call JobWorker actor to destroy without reconstruction.
   *
   * @param actor target JobWorker actor
   * @return destroy result
   */
  public static Boolean shutdownWithoutReconstruction(BaseActor actor) {
    LOG.info("Call worker to shutdown without reconstruction, actor is {}.",
        actor.getId());
    Boolean result = false;

    // TODO (datayjz): ray call worker to destroy

    LOG.info("Finished calling wk shutdownWithoutReconstruction, result is {}.", result);
    return result;
  }

}
