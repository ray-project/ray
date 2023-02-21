import { Color } from "@material-ui/core";
import {
  blue,
  blueGrey,
  cyan,
  green,
  grey,
  lightBlue,
  red,
} from "@material-ui/core/colors";
import { CSSProperties } from "@material-ui/core/styles/withStyles";
import React, { ReactNode } from "react";
import { ActorEnum } from "../type/actor";
import { PlacementGroupState } from "../type/placementGroup";
import { TypeTaskStatus } from "../type/task";

const colorMap = {
  node: {
    ALIVE: green,
    DEAD: red,
  },
  worker: {
    ALIVE: green,
  },
  actor: {
    [ActorEnum.ALIVE]: green,
    [ActorEnum.DEAD]: red,
    [ActorEnum.PENDING]: blue,
    [ActorEnum.RECONSTRUCTING]: lightBlue,
  },
  task: {
    [TypeTaskStatus.FAILED]: red,
    [TypeTaskStatus.FINISHED]: green,
    [TypeTaskStatus.RUNNING]: blue,
    [TypeTaskStatus.RUNNING_IN_RAY_GET]: blue,
    [TypeTaskStatus.RUNNING_IN_RAY_WAIT]: blue,
    [TypeTaskStatus.SUBMITTED_TO_WORKER]: "#cfcf08",
    [TypeTaskStatus.PENDING_ARGS_FETCH]: blue,
    [TypeTaskStatus.PENDING_OBJ_STORE_MEM_AVAIL]: blue,
    [TypeTaskStatus.PENDING_NODE_ASSIGNMENT]: "#cfcf08",
    [TypeTaskStatus.PENDING_ARGS_AVAIL]: "#f79e02",
  },
  job: {
    INIT: grey,
    SUBMITTED: "#cfcf08",
    DISPATCHED: lightBlue,
    RUNNING: blue,
    COMPLETED: green,
    SUCCEEDED: green,
    FINISHED: green,
    FAILED: red,
  },
  placementGroup: {
    [PlacementGroupState.PENDING]: "#f79e02",
    [PlacementGroupState.CREATED]: blue,
    [PlacementGroupState.REMOVED]: red,
    [PlacementGroupState.RESCHEDULING]: "#cfcf08",
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

export const StatusChip = ({
  type,
  status,
  suffix,
}: {
  type: string;
  status: string | ActorEnum | ReactNode;
  suffix?: string;
}) => {
  const style = {
    padding: "2px 8px",
    border: "solid 1px",
    borderRadius: 4,
    fontSize: 12,
    margin: 2,
  } as CSSProperties;

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

  style.color = colorValue;
  style.borderColor = colorValue;
  if (color !== blueGrey) {
    style.backgroundColor = `${colorValue}20`;
  }

  return (
    <span style={style}>
      {status}
      {suffix}
    </span>
  );
};
