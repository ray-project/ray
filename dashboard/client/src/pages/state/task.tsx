import { Grid } from "@material-ui/core";
import dayjs from "dayjs";
import React, { useState } from "react";
import TaskTable from "../../components/TaskTable";
import { getTasks } from "../../service/task";
import { Task } from "../../type/task";
import { useStateApiList } from "./hook/useStateApi";

/**
 * Represent the embedable actors page.
 */
const TaskList = ({ jobId = null }: { jobId?: string | null }) => {
  const [timeStamp] = useState(dayjs());
  const data: Array<Task> | undefined = useStateApiList("useTasks", getTasks);
  const tasks = data ? data : [];

  return (
    <div>
      <Grid container alignItems="center">
        <Grid item>{timeStamp.format("YYYY-MM-DD HH:mm:ss")}</Grid>
      </Grid>
      <TaskTable tasks={tasks} jobId={jobId} />
    </div>
  );
};

export default TaskList;
