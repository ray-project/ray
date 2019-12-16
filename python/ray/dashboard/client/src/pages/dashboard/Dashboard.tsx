import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import NodeInfo from "./node-info/NodeInfo";
import RayConfig from "./ray-config/RayConfig";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: theme.palette.background.paper,
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(4)
      }
    }
  });

class Dashboard extends React.Component<WithStyles<typeof styles>> {
  render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <Typography variant="h5">Ray Dashboard</Typography>
        <NodeInfo />
        <RayConfig />
      </div>
    );
  }
}

export default withStyles(styles)(Dashboard);
