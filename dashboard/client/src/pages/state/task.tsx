import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import TaskTable, { TaskTableProps } from "../../components/TaskTable";
import { getTasks } from "../../service/task";
import { Task } from "../../type/task";
import { useStateApiList } from "./hook/useStateApi";

/**
 * Represent the embedable tasks page.
 */
const TaskList = ({
  jobId,
  actorId,
  ...taskTableProps
}: {
  jobId?: string;
  actorId?: string;
} & Pick<TaskTableProps, "filterToTaskId" | "onFilterChange" | "newIA">) => {
  const [timeStamp] = useState(dayjs());
  const data: Task[] | undefined = useStateApiList("useTasks", () =>
    getTasks(jobId),
  );
  const tasks = data ? data : [];

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>
          Last updated: {timeStamp.format("YYYY-MM-DD HH:mm:ss")}
        </Grid>
      </Grid>
      <TaskTable
        tasks={tasks}
        jobId={jobId}
        actorId={actorId}
        {...taskTableProps}
      />
    </div>
  );
};

export default TaskList;
