import { Box, Typography } from "@mui/material";
import React from "react";
import { useParams } from "react-router-dom";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { generateActorLink, generateNodeLink } from "../../common/links";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import {
  TaskCpuProfilingLink,
  TaskCpuStackTraceLink,
  TaskMemoryProfilingButton,
} from "../../common/ProfilingLink";
import { Section } from "../../common/Section";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { Task } from "../../type/task";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useStateApiTask } from "../state/hook/useStateApi";

export const TaskPage = () => {
  const { taskId } = useParams();
  const { task, isLoading } = useStateApiTask(taskId);

  return (
    <Box sx={{ padding: 2, backgroundColor: "white" }}>
      <MainNavPageInfo
        pageInfo={
          task
            ? {
                id: "task",
                title: task.task_id,
                pageTitle: `${task.task_id} | Task`,
                path: `tasks/${task.task_id}`,
              }
            : {
                id: "task",
                title: "Task",
                path: `tasks/${taskId}`,
              }
        }
      />
      <TaskPageContents taskId={taskId} task={task} isLoading={isLoading} />
    </Box>
  );
};

type TaskPageContentsProps = {
  taskId?: string;
  task?: Task;
  isLoading: boolean;
};

const TaskPageContents = ({
  taskId,
  task,
  isLoading,
}: TaskPageContentsProps) => {
  if (isLoading) {
    return <Loading loading />;
  }

  if (!task) {
    return (
      <Typography color="error">Task with ID "{taskId}" not found.</Typography>
    );
  }

  const {
    attempt_number,
    task_id,
    actor_id,
    end_time_ms,
    start_time_ms,
    node_id,
    placement_group_id,
    required_resources,
    state,
    type,
    worker_id,
    job_id,
    func_or_class_name,
    name,
    call_site,
    label_selector,
  } = task;
  const isTaskActive = task.state === "RUNNING" && task.worker_id;

  return (
    <div>
      <MetadataSection
        metadataList={[
          {
            label: "ID",
            content: {
              value: task_id,
              copyableValue: task_id,
            },
          },
          {
            label: "Name",
            content: {
              value: name,
            },
          },
          {
            label: "State",
            content: <StatusChip type="task" status={state} />,
          },
          {
            label: "Job ID",
            content: {
              value: job_id,
              copyableValue: job_id,
            },
          },
          {
            label: "Function or class name",
            content: {
              value: func_or_class_name,
            },
          },
          {
            label: "Actor ID",
            content: actor_id
              ? {
                  value: actor_id,
                  copyableValue: actor_id,
                  link: generateActorLink(actor_id),
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Node ID",
            content: node_id
              ? {
                  value: node_id,
                  copyableValue: node_id,
                  link: generateNodeLink(node_id),
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Worker ID",
            content: worker_id
              ? {
                  value: worker_id,
                  copyableValue: worker_id,
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Type",
            content: {
              value: type,
            },
          },
          {
            label: "Placement group ID",
            content: placement_group_id
              ? {
                  value: placement_group_id,
                  copyableValue: placement_group_id,
                }
              : {
                  value: "-",
                },
          },
          {
            label: "Required resources",
            content:
              Object.entries(required_resources).length > 0 ? (
                <Box display="inline-block">
                  <CodeDialogButtonWithPreview
                    title="Required resources"
                    code={JSON.stringify(required_resources, undefined, 2)}
                  />
                </Box>
              ) : (
                {
                  value: "{}",
                }
              ),
          },
          {
            label: "Label Selector",
            content: (
              <Box display="inline-block">
                {Object.entries(label_selector || {}).length > 0 ? (
                  <CodeDialogButtonWithPreview
                    title="Label selector"
                    code={JSON.stringify(label_selector, undefined, 2)}
                  />
                ) : (
                  "{}"
                )}
              </Box>
            ),
          },
          {
            label: "Started at",
            content: {
              value: start_time_ms ? formatDateFromTimeMs(start_time_ms) : "-",
            },
          },
          {
            label: "Ended at",
            content: {
              value: end_time_ms ? formatDateFromTimeMs(end_time_ms) : "-",
            },
          },
          {
            label: "Duration",
            content: start_time_ms ? (
              <DurationText startTime={start_time_ms} endTime={end_time_ms} />
            ) : (
              {
                value: "-",
              }
            ),
          },
          isTaskActive
            ? {
                label: "Actions",
                content: (
                  <React.Fragment>
                    <TaskCpuProfilingLink
                      taskId={task_id}
                      attemptNumber={attempt_number}
                      nodeId={node_id}
                    />
                    <br />
                    <TaskCpuStackTraceLink
                      taskId={task_id}
                      attemptNumber={attempt_number}
                      nodeId={node_id}
                    />
                    <br />
                    <TaskMemoryProfilingButton
                      taskId={task_id}
                      attemptNumber={attempt_number}
                      nodeId={node_id}
                    />
                  </React.Fragment>
                ),
              }
            : {
                label: "",
                content: undefined,
              },
          {
            label: "Call site",
            content: (
              <Box display="inline-block">
                <CodeDialogButton
                  title="Call site"
                  code={
                    call_site ||
                    'Call site not recorded. To enable, set environment variable "RAY_record_task_actor_creation_sites" to "true".'
                  }
                />
              </Box>
            ),
          },
        ]}
      />
      <CollapsibleSection title="Logs" startExpanded>
        <Section noTopPadding>
          <TaskLogs task={task} />
        </Section>
      </CollapsibleSection>
    </div>
  );
};

type TaskLogsProps = {
  task: Task;
};

const TaskLogs = ({
  task: {
    task_id,
    error_message,
    error_type,
    worker_id,
    task_log_info,
    actor_id,
  },
}: TaskLogsProps) => {
  const errorDetails =
    error_type !== null && error_message !== null
      ? `Error Type: ${error_type}\n\n${error_message}`
      : undefined;

  const otherLogsLink =
    task_log_info === null && actor_id !== null
      ? `/actors/${actor_id}`
      : undefined;

  const tabs: MultiTabLogViewerTabDetails[] = [
    ...(worker_id !== null && task_log_info !== null
      ? ([
          {
            title: "stderr",
            taskId: task_id,
            suffix: "err",
          },
          {
            title: "stdout",
            taskId: task_id,
            suffix: "out",
          },
        ] as const)
      : []),
    ...(task_log_info === null
      ? [
          {
            title: "Logs",
            contents:
              "Logs of actor tasks are only available " +
              "as part of the actor logs by default due to performance reason. " +
              'Please click "Other logs" link above to access the actor logs. \n' +
              "To record actor task log by default, you could set the runtime env of the actor or start the cluster with RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1 ",
          },
        ]
      : []),
    // TODO(aguo): uncomment once PID is available in the API.
    // {
    //   title: "system",
    //   nodeId: node_id,
    //   // TODO(aguo): Have API return the log file name.
    //   filename: `python-core-worker-${worker_id}_${pid}.log`,
    // },
    ...(errorDetails
      ? [{ title: "Error stack trace", contents: errorDetails }]
      : []),
  ];
  return (
    <MultiTabLogViewer
      tabs={tabs}
      otherLogsLink={otherLogsLink}
      contextKey="tasks-page"
    />
  );
};
