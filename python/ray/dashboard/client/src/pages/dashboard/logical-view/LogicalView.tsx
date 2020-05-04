import {
  createStyles,
  Theme,
  Typography,
  WithStyles,
  withStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { StoreState } from "../../../store";
import Actors from "./Actors";

const styles = (theme: Theme) =>
  createStyles({
    warning: {
      fontSize: "0.8125rem",
      marginBottom: theme.spacing(2),
    },
    warningIcon: {
      fontSize: "1.25em",
      verticalAlign: "text-bottom",
    },
  });

const mapStateToProps = (state: StoreState) => ({
  rayletInfo: state.dashboard.rayletInfo,
});

class LogicalView extends React.Component<
  WithStyles<typeof styles> & ReturnType<typeof mapStateToProps>
> {
  render() {
    const { rayletInfo } = this.props;
    return (
      <div>
        {rayletInfo === null ? (
          <Typography color="textSecondary">Loading...</Typography>
        ) : Object.entries(rayletInfo.actors).length === 0 ? (
          <Typography color="textSecondary">No actors found.</Typography>
        ) : (
          <Actors actors={rayletInfo.actors} />
        )}
      </div>
    );
  }
}

export default connect(mapStateToProps)(withStyles(styles)(LogicalView));
