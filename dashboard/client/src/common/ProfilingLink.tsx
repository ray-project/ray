import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

type CpuProfilingLinkProps = PropsWithChildren<
  {
    pid: string | number | null | undefined;
    ip: string | null | undefined;
    type: string | null;
  } & ClassNameProps
>;

export const CpuProfilingLink = ({
  pid,
  ip,
  type = "",
}: CpuProfilingLinkProps) => {
  if (!pid || !ip || typeof pid === "undefined" || typeof ip === "undefined") {
    return <div></div>;
  }

  return (
    <a
      href={`worker/traceback?pid=${pid}&ip=${ip}&native=0`}
      target="_blank"
      title="Sample the current Python stack trace for this worker."
      rel="noreferrer"
    >
      Stack&nbsp;Trace{type ? ` (${type})` : ""}
    </a>
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
    <a
      href={`worker/cpu_profile?pid=${pid}&ip=${ip}&duration=5&native=0`}
      target="_blank"
      title="Profile the Python worker for 5 seconds (default) and display a CPU flame graph."
      rel="noreferrer"
    >
      CPU&nbsp;Flame&nbsp;Graph{type ? ` (${type})` : ""}
    </a>
  );
};
