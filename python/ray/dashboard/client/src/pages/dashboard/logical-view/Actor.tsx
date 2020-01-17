import Typography from "@material-ui/core/Typography";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import React from "react";
import { RayletInfoResponse } from "../../../api";
import Actors from "./Actors";
import Collapse from "@material-ui/core/Collapse";

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
    expandCollapseButton: {
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
}

class Actor extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    expanded: true
  };

  setExpanded = (expanded: boolean) => () => {
    this.setState({ expanded });
  };

  render() {
    const { classes, actor } = this.props;
    const { expanded } = this.state;

    const information =
      actor.state !== -1
        ? [
            {
              label: "ID",
              value: actor.actorId
            },
            {
              label: "Resources",
              value:
                Object.entries(actor.usedResources).length > 0 &&
                Object.entries(actor.usedResources)
                  .map(([key, value]) => `${value.toLocaleString()} ${key}`)
                  .join(", ")
            },
            {
              label: "Pending",
              value:
                actor.taskQueueLength !== undefined &&
                actor.taskQueueLength > 0 &&
                actor.taskQueueLength.toLocaleString()
            },
            {
              label: "Task",
              value:
                actor.currentTaskFuncDesc && actor.currentTaskFuncDesc.join(".")
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
                    className={classes.expandCollapseButton}
                    onClick={this.setExpanded(!expanded)}
                  >
                    {expanded ? "Collapse" : "Expand"}
                  </span>
                  )
                </React.Fragment>
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
