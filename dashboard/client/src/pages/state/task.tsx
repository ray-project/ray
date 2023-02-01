import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import TaskTable from "../../components/TaskTable";
import { getTasks } from "../../service/task";
import { Task } from "../../type/task";
import { useStateApiList } from "./hook/useStateApi";

/**
 * Represent the embedable tasks page.
 */
const TaskList = ({
  jobId = null,
  actorId = null,
}: {
  jobId?: string | null;
  actorId?: string | null;
}) => {
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
      <TaskTable tasks={tasks} jobId={jobId} actorId={actorId} />
    </div>
  );
};

export default TaskList;
