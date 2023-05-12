import { createStyles, makeStyles, Typography } from "@material-ui/core";
import React from "react";
import { useParams } from "react-router-dom";
import { CodeDialogButton } from "../../common/CodeDialogButton";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { generateActorLink, generateNodeLink } from "../../common/links";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTab,
} from "../../common/MultiTabLogViewer";
import { Section } from "../../common/Section";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import { Task } from "../../type/task";
import { MainNavPageInfo } from "../layout/mainNavContext";
import { useStateApiTask } from "../state/hook/useStateApi";

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      padding: theme.spacing(2),
      backgroundColor: "white",
    },
  }),
);

export const TaskPage = () => {
  const { taskId } = useParams();
  const { task, isLoading } = useStateApiTask(taskId);

  const classes = useStyles();

  return (
    <div className={classes.root}>
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
    </div>
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
  } = task;

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
            content: {
              value: actor_id ? actor_id : "-",
              copyableValue: actor_id ? actor_id : undefined,
              link: actor_id ? generateActorLink(actor_id) : undefined,
            },
          },
          {
            label: "Node ID",
            content: {
              value: node_id ? node_id : "-",
              copyableValue: node_id ? node_id : undefined,
              link: node_id ? generateNodeLink(node_id) : undefined,
            },
          },
          {
            label: "Worker ID",
            content: {
              value: worker_id ? worker_id : "-",
              copyableValue: worker_id ? worker_id : undefined,
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
            content: {
              value: placement_group_id ? placement_group_id : "-",
              copyableValue: placement_group_id
                ? placement_group_id
                : undefined,
            },
          },
          {
            label: "Required resources",
            content:
              Object.entries(required_resources).length > 0 ? (
                <CodeDialogButton
                  title="Required resources"
                  code={JSON.stringify(required_resources, undefined, 2)}
                />
              ) : (
                {
                  value: "{}",
                }
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
        ]}
      />
      <CollapsibleSection title="Logs" startExpanded>
        <Section>
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
  task: { node_id, worker_id, job_id, error_message, error_type },
}: TaskLogsProps) => {
  if (!node_id || !worker_id || !job_id) {
    return <Typography color="error">No logs available.</Typography>;
  }

  const errorDetails =
    error_type !== null && error_message !== null
      ? `Error Type: ${error_type}\n\n${error_message}`
      : undefined;

  const tabs: MultiTabLogViewerTab[] = [
    // {
    //   title: "stderr",
    //   nodeId: node_id,
    //   // TODO(aguo): Have API return the log file name.
    //   filename: `worker-${worker_id}-${job_id}-${pid}.err`,
    // },
    // {
    //   title: "stdout",
    //   nodeId: node_id,
    //   // TODO(aguo): Have API return the log file name.
    //   filename: `worker-${worker_id}-${job_id}-${pid}.out`,
    // },
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
  return <MultiTabLogViewer tabs={tabs} />;
};
