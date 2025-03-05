import { Box, Color } from "@mui/material";
import { blue, blueGrey, cyan, green, red } from "@mui/material/colors";
import React, { CSSProperties, ReactNode } from "react";
import { TaskStatus } from "../pages/job/hook/useJobProgress";
import { ActorEnum } from "../type/actor";
import { JobStatus } from "../type/job";
import { PlacementGroupState } from "../type/placementGroup";
import {
  ServeApplicationStatus,
  ServeDeploymentStatus,
  ServeReplicaState,
  ServeSystemActorStatus,
} from "../type/serve";

const orange = "#DB6D00";
const grey = "#5F6469";

const colorMap = {
  node: {
    ALIVE: green,
    DEAD: grey,
  },
  worker: {
    ALIVE: green,
    DEAD: grey,
  },
  actor: {
    [ActorEnum.ALIVE]: green,
    [ActorEnum.DEAD]: grey,
    [ActorEnum.DEPENDENCIES_UNREADY]: orange,
    [ActorEnum.PENDING_CREATION]: orange,
    [ActorEnum.RESTARTING]: orange,
  },
  task: {
    [TaskStatus.FAILED]: red,
    [TaskStatus.FINISHED]: green,
    [TaskStatus.RUNNING]: blue,
    [TaskStatus.SUBMITTED_TO_WORKER]: orange,
    [TaskStatus.PENDING_NODE_ASSIGNMENT]: orange,
    [TaskStatus.PENDING_ARGS_AVAIL]: orange,
    [TaskStatus.UNKNOWN]: grey,
  },
  job: {
    [JobStatus.PENDING]: orange,
    [JobStatus.RUNNING]: blue,
    [JobStatus.STOPPED]: grey,
    [JobStatus.SUCCEEDED]: green,
    [JobStatus.FAILED]: red,
  },
  placementGroup: {
    [PlacementGroupState.PENDING]: orange,
    [PlacementGroupState.CREATED]: green,
    [PlacementGroupState.REMOVED]: grey,
    [PlacementGroupState.RESCHEDULING]: orange,
  },
  serveApplication: {
    [ServeApplicationStatus.NOT_STARTED]: grey,
    [ServeApplicationStatus.DEPLOYING]: orange,
    [ServeApplicationStatus.RUNNING]: green,
    [ServeApplicationStatus.DEPLOY_FAILED]: red,
    [ServeApplicationStatus.DELETING]: orange,
    [ServeApplicationStatus.UNHEALTHY]: red,
  },
  serveDeployment: {
    [ServeDeploymentStatus.UPDATING]: orange,
    [ServeDeploymentStatus.HEALTHY]: green,
    [ServeDeploymentStatus.UNHEALTHY]: red,
  },
  serveReplica: {
    [ServeReplicaState.STARTING]: orange,
    [ServeReplicaState.UPDATING]: orange,
    [ServeReplicaState.RECOVERING]: orange,
    [ServeReplicaState.RUNNING]: green,
    [ServeReplicaState.STOPPING]: red,
  },
  serveProxy: {
    [ServeSystemActorStatus.HEALTHY]: green,
    [ServeSystemActorStatus.UNHEALTHY]: red,
    [ServeSystemActorStatus.STARTING]: orange,
    [ServeSystemActorStatus.DRAINING]: blueGrey,
  },
  serveController: {
    [ServeSystemActorStatus.HEALTHY]: green,
    [ServeSystemActorStatus.UNHEALTHY]: red,
    [ServeSystemActorStatus.STARTING]: orange,
  },
} as {
  [key: string]: {
    [key: string]: Color | string;
  };
};

const typeMap = {
  deps: blue,
  INFO: cyan,
  ERROR: red,
} as {
  [key: string]: Color;
};

export type StatusChipProps = {
  type: keyof typeof colorMap;
  status: string | ActorEnum | ReactNode;
  suffix?: ReactNode;
  icon?: ReactNode;
};

export const StatusChip = ({ type, status, suffix, icon }: StatusChipProps) => {
  let color: Color | string = blueGrey;

  if (typeMap[type]) {
    color = typeMap[type];
  } else if (
    typeof status === "string" &&
    colorMap[type] &&
    colorMap[type][status]
  ) {
    color = colorMap[type][status];
  }

  const colorValue = typeof color === "string" ? color : color[500];

  const style: CSSProperties = {};
  style.color = colorValue;
  style.borderColor = colorValue;
  if (color !== blueGrey) {
    style.backgroundColor = `${colorValue}20`;
  }

  return (
    <Box
      component="span"
      sx={{
        padding: "2px 8px",
        border: "solid 1px",
        borderRadius: "4px",
        fontSize: 12,
        display: "inline-flex",
        alignItems: "center",
      }}
      style={style}
    >
      {icon}
      <Box
        component="span"
        sx={icon !== undefined ? { marginLeft: "4px" } : {}}
      >
        {status}
      </Box>
      <Box sx={{ marginLeft: 0.5 }}>{suffix}</Box>
    </Box>
  );
};
