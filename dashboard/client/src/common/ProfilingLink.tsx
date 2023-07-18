import { Link } from "@material-ui/core";
import React, { PropsWithChildren, useState } from "react";
import { ClassNameProps } from "./props";

type CpuProfilingLinkProps = PropsWithChildren<
  {
    pid: string | number | null | undefined;
    ip: string | null | undefined;
    type: string | null;
  } & ClassNameProps
>;

type TaskProfilingStackTraceProps = {
  taskId: string | null | undefined;
  attemptNumber: number;
};

export const TaskCpuProfilingLink = ({
  taskId,
  attemptNumber,
}: TaskProfilingStackTraceProps) => {
  const [svgContent, setSvgContent] = useState("");
  const [warning, setWarning] = useState("");
  if (!taskId) {
    return null;
  }
  const handleClick = async (taskId: any) => {
    if (!taskId) {
      return;
    }
    console.info("taskId: ", taskId);
    const newWindow = window.open("", "_blank") as Window;
    newWindow.document.write(`<p>${warning}</p>${svgContent}`);
    newWindow.document.close();
    const startTime = performance.now();

    const response = await fetch(
      `task/cpu_profile?task_id=${taskId}&attempt_number=${attemptNumber}`,
    );
    const endTime = performance.now();
    const elapsedTime = endTime - startTime;
    console.info("Elapsed time (ms): ", elapsedTime);

    console.info("response: ", response);

    const data = await response.json();
    console.info("data: ", data);

    setSvgContent(data.content);
    setWarning(data.warning);


  };
  return (
    <div onClick={() => handleClick(taskId)}>
      <Link title="Profile the Python worker for 5 seconds (default) and display a CPU flame graph.">
        CPU&nbsp;Flame&nbsp;Graph
      </Link>
    </div>
  );
};

export const TaskCpuStackTraceLink = ({
  taskId,
  attemptNumber,
}: TaskProfilingStackTraceProps) => {
  if (!taskId) {
    return null;
  }
  return (
    <Link
      href={`task/traceback?task_id=${taskId}&attempt_number=${attemptNumber}`}
      target="_blank"
      title="Sample the current Python stack trace for this worker."
      rel="noreferrer"
    >
      Stack&nbsp;Trace
    </Link>
  );
};

export const CpuProfilingLink = ({
  pid,
  ip,
  type = "",
}: CpuProfilingLinkProps) => {
  if (!pid || !ip || typeof pid === "undefined" || typeof ip === "undefined") {
    return <div></div>;
  }
  return (
    <Link
      href={`worker/traceback?pid=${pid}&ip=${ip}&native=0`}
      target="_blank"
      title="Sample the current Python stack trace for this worker."
      rel="noreferrer"
    >
      Stack&nbsp;Trace{type ? ` (${type})` : ""}
    </Link>
  );
};

export const CpuStackTraceLink = ({
  pid,
  ip,
  type = "",
}: CpuProfilingLinkProps) => {
  if (!pid || !ip) {
    return <div></div>;
  }

  return (
    <Link
      href={`worker/cpu_profile?pid=${pid}&ip=${ip}&duration=5&native=0`}
      target="_blank"
      title="Profile the Python worker for 5 seconds (default) and display a CPU flame graph."
      rel="noreferrer"
    >
      CPU&nbsp;Flame&nbsp;Graph{type ? ` (${type})` : ""}
    </Link>
  );
};
