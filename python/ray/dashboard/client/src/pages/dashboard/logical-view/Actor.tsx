import Collapse from "@material-ui/core/Collapse";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import {
  checkProfilingStatus,
  CheckProfilingStatusResponse,
  launchProfiling,
  RayletInfoResponse
} from "../../../api";
import Actors from "./Actors";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      borderColor: theme.palette.divider,
      borderStyle: "solid",
      borderWidth: 1,
      marginTop: theme.spacing(2),
      padding: theme.spacing(2)
    },
    title: {
      color: theme.palette.text.secondary,
      fontSize: "0.75rem"
    },
    infeasible: {
      color: theme.palette.error.main
    },
    information: {
      fontSize: "0.875rem"
    },
    datum: {
      "&:not(:first-child)": {
        marginLeft: theme.spacing(2)
      }
    },
    webuiDisplay: {
      fontSize: "0.875rem"
    },
    action: {
      color: theme.palette.primary.main,
      "&:hover": {
        cursor: "pointer"
      }
    }
  });

interface Props {
  actor: RayletInfoResponse["actors"][keyof RayletInfoResponse["actors"]];
}

interface State {
  expanded: boolean;
  profiling: { [profilingId: string]: CheckProfilingStatusResponse | null };
}

class Actor extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    expanded: true,
    profiling: {}
  };

  setExpanded = (expanded: boolean) => () => {
    this.setState({ expanded });
  };

  handleProfilingClick = async () => {
    const actor = this.props.actor;
    if (actor.state !== -1) {
      const duration = 10;
      const profilingId = await launchProfiling(
        actor.nodeId,
        actor.pid,
        duration
      );
      this.setState(state => ({
        profiling: { ...state.profiling, [profilingId]: null }
      }));
      const checkProfilingStatusLoop = async () => {
        const response = await checkProfilingStatus(profilingId);
        this.setState(state => ({
          profiling: { ...state.profiling, [profilingId]: response }
        }));
        if (response.status === "pending") {
          setTimeout(checkProfilingStatusLoop, 1000);
        }
      };
      await checkProfilingStatusLoop();
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
              value: actor.actorTitle
            },
            {
              label: "State",
              value: actor.state.toLocaleString()
            },
            {
              label: "Resources",
              value:
                Object.entries(actor.usedResources).length > 0 &&
                Object.entries(actor.usedResources)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", ")
            },
            {
              label: "Pending",
              value: actor.taskQueueLength.toLocaleString()
            },
            {
              label: "Executed",
              value: actor.numExecutedTasks.toLocaleString()
            },
            {
              label: "NumObjectIdsInScope",
              value: actor.numObjectIdsInScope.toLocaleString()
            },
            {
              label: "NumLocalObjects",
              value: actor.numLocalObjects.toLocaleString()
            },
            {
              label: "UsedLocalObjectMemory",
              value: actor.usedObjectStoreMemory.toLocaleString()
            },
            {
              label: "Task",
              value: actor.currentTaskFuncDesc.join(".")
            }
          ]
        : [
            {
              label: "ID",
              value: actor.actorId
            },
            {
              label: "Required resources",
              value:
                Object.entries(actor.requiredResources).length > 0 &&
                Object.entries(actor.requiredResources)
                  .sort((a, b) => a[0].localeCompare(b[0]))
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", ")
            }
          ];

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
              (
              <span
                className={classes.action}
                onClick={this.handleProfilingClick}
              >
                Profile
              </span>
              ){" "}
              {Object.entries(profiling).map(
                ([profilingId, profilingStatus]) =>
                  profilingStatus !== null && (
                    <React.Fragment>
                      ({profilingId}:{" "}
                      {profilingStatus.status === "finished" ? (
                        <a
                          href={`${
                            window.origin
                          }/speedscope/index.html#profileURL=${encodeURIComponent(
                            `${window.origin}/api/get_profiling_info?profiling_id=${profilingId}`
                          )}`}
                        >
                          {profilingStatus.status}
                        </a>
                      ) : profilingStatus.status === "error" ? (
                        profilingStatus.error.trim()
                      ) : (
                        profilingStatus.status
                      )}
                      ){" "}
                    </React.Fragment>
                  )
              )}
            </React.Fragment>
          ) : (
            <span className={classes.infeasible}>Infeasible actor</span>
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
              )
          )}
        </Typography>
        {actor.state !== -1 && (
          <React.Fragment>
            {actor.webuiDisplay && (
              <Typography className={classes.webuiDisplay}>
                {actor.webuiDisplay}
              </Typography>
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
