import {
  Collapse,
  createStyles,
  Theme,
  Typography,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import {
  checkProfilingStatus,
  CheckProfilingStatusResponse,
  getProfilingResultURL,
  launchKillActor,
  launchProfiling,
  RayletInfoResponse,
} from "../../../api";
import Actors from "./Actors";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      borderColor: theme.palette.divider,
      borderStyle: "solid",
      borderWidth: 1,
      marginTop: theme.spacing(2),
      padding: theme.spacing(2),
    },
    title: {
      color: theme.palette.text.secondary,
      fontSize: "0.75rem",
    },
    action: {
      color: theme.palette.primary.main,
      textDecoration: "none",
      "&:hover": {
        cursor: "pointer",
      },
    },
    invalidStateTypeInfeasible: {
      color: theme.palette.error.main,
    },
    invalidStateTypePendingActor: {
      color: theme.palette.secondary.main,
    },
    information: {
      fontSize: "0.875rem",
    },
    datum: {
      "&:not(:first-child)": {
        marginLeft: theme.spacing(2),
      },
    },
    webuiDisplay: {
      fontSize: "0.875rem",
    },
    inlineHTML: {
      fontSize: "0.875rem",
      display: "inline",
    },
  });

type Props = {
  actor: RayletInfoResponse["actors"][keyof RayletInfoResponse["actors"]];
};

type State = {
  expanded: boolean;
  profiling: {
    [profilingId: string]: {
      startTime: number;
      latestResponse: CheckProfilingStatusResponse | null;
    };
  };
};

class Actor extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    expanded: true,
    profiling: {},
  };

  setExpanded = (expanded: boolean) => () => {
    this.setState({ expanded });
  };

  handleProfilingClick = (duration: number) => async () => {
    const actor = this.props.actor;
    if (actor.state !== -1) {
      const profilingId = await launchProfiling(
        actor.nodeId,
        actor.pid,
        duration,
      );
      this.setState((state) => ({
        profiling: {
          ...state.profiling,
          [profilingId]: { startTime: Date.now(), latestResponse: null },
        },
      }));
      const checkProfilingStatusLoop = async () => {
        const response = await checkProfilingStatus(profilingId);
        this.setState((state) => ({
          profiling: {
            ...state.profiling,
            [profilingId]: {
              ...state.profiling[profilingId],
              latestResponse: response,
            },
          },
        }));
        if (response.status === "pending") {
          setTimeout(checkProfilingStatusLoop, 1000);
        }
      };
      await checkProfilingStatusLoop();
    }
  };

  killActor = () => {
    const actor = this.props.actor;
    if (actor.state === 0) {
      launchKillActor(actor.actorId, actor.ipAddress, actor.port);
    }
  };

  render() {
    const { classes, actor } = this.props;
    const { expanded, profiling } = this.state;

    const information =
      actor.state !== -1
        ? [
            {
              label: "ActorTitle",
              value: actor.actorTitle,
            },
            {
              label: "State",
              value: actor.state.toLocaleString(),
            },
            {
              label: "Resources",
              value:
                Object.entries(actor.usedResources).length > 0 &&
                Object.entries(actor.usedResources)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", "),
            },
            {
              label: "Pending",
              value: actor.taskQueueLength.toLocaleString(),
            },
            {
              label: "Executed",
              value: actor.numExecutedTasks.toLocaleString(),
            },
            {
              label: "NumObjectIdsInScope",
              value: actor.numObjectIdsInScope.toLocaleString(),
            },
            {
              label: "NumLocalObjects",
              value: actor.numLocalObjects.toLocaleString(),
            },
            {
              label: "UsedLocalObjectMemory",
              value: actor.usedObjectStoreMemory.toLocaleString(),
            },
            // {
            //   label: "Task",
            //   value: actor.currentTaskFuncDesc.join(".")
            // }
          ]
        : [
            {
              label: "ID",
              value: actor.actorId,
            },
            {
              label: "Required resources",
              value:
                Object.entries(actor.requiredResources).length > 0 &&
                Object.entries(actor.requiredResources)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", "),
            },
          ];

    // Construct the custom message from the actor.
    let actorCustomDisplay: JSX.Element[] = [];
    if (actor.state !== -1 && actor.webuiDisplay) {
      actorCustomDisplay = Object.keys(actor.webuiDisplay)
        .sort()
        .map((key, _, __) => {
          // Construct the value from actor.
          // Please refer to worker.py::show_in_webui for schema.
          const valueEncoded = actor.webuiDisplay![key];
          const valueParsed = JSON.parse(valueEncoded);
          let valueRendered = valueParsed["message"];
          if (valueParsed["dtype"] === "html") {
            valueRendered = (
              <div
                className={classes.inlineHTML}
                dangerouslySetInnerHTML={{ __html: valueRendered }}
              ></div>
            );
          }

          if (key === "") {
            return (
              <Typography className={classes.webuiDisplay}>
                &nbsp; &nbsp; {valueRendered}
              </Typography>
            );
          } else {
            return (
              <Typography className={classes.webuiDisplay}>
                &nbsp; &nbsp; {key}: {valueRendered}
              </Typography>
            );
          }
        });
    }

    return (
      <div className={classes.root}>
        <Typography className={classes.title}>
          {actor.state !== -1 ? (
            <React.Fragment>
              Actor {actor.actorId}{" "}
              {Object.entries(actor.children).length > 0 && (
                <React.Fragment>
                  (
                  <span
                    className={classes.action}
                    onClick={this.setExpanded(!expanded)}
                  >
                    {expanded ? "Collapse" : "Expand"}
                  </span>
                  )
                </React.Fragment>
              )}{" "}
              (Profile for
              {[10, 30, 60].map((duration) => (
                <React.Fragment>
                  {" "}
                  <span
                    className={classes.action}
                    onClick={this.handleProfilingClick(duration)}
                  >
                    {duration}s
                  </span>
                </React.Fragment>
              ))}
              ){" "}
              {actor.state === 0 && (
                <span className={classes.action} onClick={this.killActor}>
                  Kill Actor
                </span>
              )}
              {Object.entries(profiling).map(
                ([profilingId, { startTime, latestResponse }]) =>
                  latestResponse !== null && (
                    <React.Fragment>
                      (
                      {latestResponse.status === "pending" ? (
                        `Profiling for ${Math.round(
                          (Date.now() - startTime) / 1000,
                        )}s...`
                      ) : latestResponse.status === "finished" ? (
                        <a
                          className={classes.action}
                          href={getProfilingResultURL(profilingId)}
                          rel="noopener noreferrer"
                          target="_blank"
                        >
                          Profiling result
                        </a>
                      ) : latestResponse.status === "error" ? (
                        `Profiling error: ${latestResponse.error.trim()}`
                      ) : undefined}
                      ){" "}
                    </React.Fragment>
                  ),
              )}
            </React.Fragment>
          ) : actor.invalidStateType === "infeasibleActor" ? (
            <span className={classes.invalidStateTypeInfeasible}>
              {actor.actorTitle} is infeasible. (This actor cannot be created
              because the Ray cluster cannot satisfy its resource requirements.)
            </span>
          ) : (
            <span className={classes.invalidStateTypePendingActor}>
              {actor.actorTitle} is pending until resources are available.
            </span>
          )}
        </Typography>
        <Typography className={classes.information}>
          {information.map(
            ({ label, value }) =>
              value &&
              value.length > 0 && (
                <React.Fragment key={label}>
                  <span className={classes.datum}>
                    {label}: {value}
                  </span>{" "}
                </React.Fragment>
              ),
          )}
        </Typography>
        {actor.state !== -1 && (
          <React.Fragment>
            {actorCustomDisplay.length > 0 && (
              <React.Fragment>{actorCustomDisplay}</React.Fragment>
            )}

            <Collapse in={expanded}>
              <Actors actors={actor.children} />
            </Collapse>
          </React.Fragment>
        )}
      </div>
    );
  }
}

export default withStyles(styles)(Actor);
