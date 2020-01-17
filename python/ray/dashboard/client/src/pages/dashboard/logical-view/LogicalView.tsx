import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { connect } from "react-redux";
import { StoreState } from "../../../store";
import Actors from "./Actors";

const styles = (theme: Theme) => createStyles({});

const mapStateToProps = (state: StoreState) => ({
  rayletInfo: state.dashboard.rayletInfo
});

class LogicalView extends React.Component<
  WithStyles<typeof styles> & ReturnType<typeof mapStateToProps>
> {
  render() {
    const { rayletInfo } = this.props;

    if (rayletInfo === null) {
      return <Typography color="textSecondary">Loading...</Typography>;
    }

    if (Object.entries(rayletInfo.actors).length === 0) {
      return <Typography color="textSecondary">No actors found.</Typography>;
    }

    return (
      <div>
        <Actors actors={rayletInfo.actors} />
      </div>
    );
  }
}

export default connect(mapStateToProps)(withStyles(styles)(LogicalView));
