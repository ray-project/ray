import React from "react";
import {
  MultiTabLogViewer,
  MultiTabLogViewerTabDetails,
} from "../../common/MultiTabLogViewer";
import { ActorDetail } from "../../type/actor";

export type ActorLogsProps = {
  actor: Pick<ActorDetail, "actorId" | "address" | "pid">;
};

export const ActorLogs = ({
  actor: {
    actorId,
    pid,
    address: { workerId, rayletId },
  },
}: ActorLogsProps) => {
  const tabs: MultiTabLogViewerTabDetails[] = [
    {
      title: "stderr",
      actorId,
      suffix: "err",
    },
    {
      title: "stdout",
      actorId,
      suffix: "out",
    },
    {
      title: "system",
      nodeId: rayletId,
      // TODO(aguo): Have API return the log file name.
      filename: `python-core-worker-${workerId}_${pid}.log`,
    },
  ];
  return <MultiTabLogViewer tabs={tabs} contextKey="actors-page" />;
};
