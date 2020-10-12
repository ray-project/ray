import { Divider, Grid, makeStyles, Theme, Typography } from "@material-ui/core";
import SpanButton from "../../../common/SpanButton";
import React, { useState } from "react";
import { ActorInfo, isFullActorInfo } from "../../../api";
import { sum } from "../../../common/util";
import LabeledDatum from "../../../common/LabeledDatum";
import ActorStateRepr from "./ActorStateRepr";
import UsageBar from '../../../common/UsageBar';
import { LogPane } from '../../../common/dialogs/logs/Logs';
import { ErrorPane } from '../../../common/dialogs/errors/Errors';
import { FullActorInfo } from '../../../../../../python/ray/new_dashboard/client/src/api';

const memoryDebuggingDocLink =
  "https://docs.ray.io/en/latest/memory-management.html#debugging-using-ray-memory";

type ActorDatum = {
  label: string;
  value: any;
  tooltip?: string;
}

const labeledActorData = (actor: ActorInfo) => (
  isFullActorInfo(actor)
    ? [
        {
          label: "Resources",
          value:
            actor.usedResources &&
            Object.entries(actor.usedResources).length > 0 &&
            Object.entries(actor.usedResources)
              .sort((a, b) => a[0].localeCompare(b[0]))
              .map(
                ([key, value]) =>
                  `${sum(
                    value.resourceSlots.map((slot) => slot.allocation),
                  )} ${key}`,
              )
              .join(", "),
        },
        {
          label: "Number of pending tasks",
          value: actor.taskQueueLength?.toLocaleString() ?? "0",
          tooltip:
            "The number of tasks that are currently pending to execute on this actor. If this number " +
            "remains consistently high, it may indicate that this actor is a bottleneck in your application.",
        },
        {
          label: "Number of executed tasks",
          value: actor.numExecutedTasks?.toLocaleString() ?? "0",
          tooltip:
            "The number of tasks this actor has executed throughout its lifetimes.",
        },
        {
          label: "Number of ObjectRefs in scope",
          value: actor.numObjectRefsInScope?.toLocaleString() ?? "0",
          tooltip:
            "The number of ObjectRefs that this actor is keeping in scope via its internal state. " +
            "This does not imply that the objects are in active use or colocated on the node with the actor " +
            `currently. This can be useful for debugging memory leaks. See the docs at ${memoryDebuggingDocLink} ` +
            "for more information.",
        },
        {
          label: "Number of local objects",
          value: actor.numLocalObjects?.toLocaleString() ?? "0",
          tooltip:
            "The number of small objects that this actor has stored in its local in-process memory store. This can be useful for " +
            `debugging memory leaks. See the docs at ${memoryDebuggingDocLink} for more information`,
        },
        {
          label: "Object store memory used (MiB)",
          value: actor.usedObjectStoreMemory?.toLocaleString() ?? "0",
          tooltip:
            "The total amount of memory that this actor is occupying in the Ray object store. " +
            "If this number is increasing without bounds, you might have a memory leak. See " +
            `the docs at: ${memoryDebuggingDocLink} for more information.`,
        },
      ]
    : [
        {
          label: "Actor ID",
          value: actor.actorId,
          tooltip: "",
        },
        {
          label: "Required resources",
          value:
            actor.requiredResources &&
            Object.entries(actor.requiredResources).length > 0 &&
            Object.entries(actor.requiredResources)
              .sort((a, b) => a[0].localeCompare(b[0]))
              .map(([key, value]) => `${value.toLocaleString()} ${key}`)
              .join(", "),
          tooltip: "",
        },
      ]);


type ActorDetailsPaneProps = {
  actor: ActorInfo;
};

const useStyles = makeStyles((theme: Theme) => ({
  divider: {
    width: "100%",
    margin: "0 auto",
  },
  actorTitleWrapper: {
    marginTop: theme.spacing(1),
    marginBottom: theme.spacing(1),
    fontWeight: "bold",
    fontSize: "130%",
  },
  detailsPane: {
    margin: theme.spacing(1),
  },
}));

const extractLogsByPid = (actor: FullActorInfo) => {
  return {[actor.processStats.pid.toString()]: actor.logs };
};
  
const extractErrorsByPid = (actor: FullActorInfo) => {
  return {[actor.processStats.pid.toString()]: actor.errors };
};

const ActorDetailsPane: React.FC<ActorDetailsPaneProps> = ({
  actor
}) => {
  const classes = useStyles();
  const [showLogs, setShowLogs] = useState(false);
  const [showErrors, setShowErrors] = useState(false);
  const actorData: ActorDatum[] = labeledActorData(actor);
  return (
    <React.Fragment>
      <div className={classes.actorTitleWrapper}>
        <div>{actor.actorClass}</div>
        <ActorStateRepr state={actor.state} />
      </div>
      {isFullActorInfo(actor) && actor.logs.length > 0 &&
        <SpanButton onClick={() => setShowLogs(true) }>
        View Logs ({actor.logs.length.toLocaleString()} {actor.logs.length === 1 ? "line" : "lines" })
        </SpanButton>
      }
      {isFullActorInfo(actor) && actor.errors.length > 0 &&
        <SpanButton onClick={() => setShowErrors(true) }>
        View errors ({actor.errors.length.toLocaleString()} {actor.errors.length === 1 ? "entry" : "entries" })
        </SpanButton>
      }
      {isFullActorInfo(actor) && showLogs &&
        <LogPane
          logs={extractLogsByPid(actor)}
          groupTag={`Actor ${actor.actorId}`}
          clearLogDialog={() => setShowLogs(false)}
          error={null}
        />
      }
      {isFullActorInfo(actor) && showErrors &&
        <ErrorPane
          errors={extractErrorsByPid(actor)}
          groupTag={`Actor ${actor.actorId}`}
          clearErrorDialog={() => setShowErrors(false)}
          fetchError={null}
        />
      }
      <Divider className={classes.divider} />
      <Grid container className={classes.detailsPane}>
        {actorData.map(
          ({ label, value, tooltip }) =>
            value &&
            value.length > 0 && (
              <LabeledDatum
                key={label}
                label={label}
                datum={value}
                tooltip={tooltip}
              />
            ),
        )}
      </Grid>
      {isFullActorInfo(actor) &&
        <Grid container className={classes.detailsPane}>
          <Grid container item xs={6}>
            <Grid item xs={12}>
              <Typography>CPU Usage</Typography>
            </Grid>
            <Grid item xs={12}>
              <UsageBar
                percent={actor.processStats?.cpuPercent ?? 0}
                text={`${actor.processStats?.cpuPercent ?? 0}`}
              />
            </Grid>
        </Grid>

        { actor.gpus.length > 0 &&
          <Grid container item xs={6}>
            <Grid item xs={12}>
              <Typography>GPU Usage</Typography>
            </Grid>
            {actor.gpus.map(gpu => (
              <React.Fragment key={gpu.uuid}>
                <Grid item xs={3}>
                  {`[${gpu.name}]`}
                </Grid>
                <Grid item xs={9}>
                  <UsageBar
                    percent={gpu.utilizationGpu * 100}
                    text={`${gpu.utilizationGpu * 100}%`}
                  />
                </Grid>
              </React.Fragment>
            ))}
          </Grid>

        }
        </Grid>}
    </React.Fragment>
  );
};

export default ActorDetailsPane;
