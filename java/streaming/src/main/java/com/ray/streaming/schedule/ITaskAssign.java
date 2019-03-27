package com.ray.streaming.schedule;

import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.runtime.StreamWorker;
import com.ray.streaming.plan.Plan;
import java.io.Serializable;
import java.util.List;
import org.ray.api.RayActor;

/**
 * Interface of the task assign.
 * Assign Logical Plan to Physical Execution Graph.
 */
public interface ITaskAssign extends Serializable {

  ExecutionGraph assign(Plan plan, List<RayActor<StreamWorker>> workers);

}
