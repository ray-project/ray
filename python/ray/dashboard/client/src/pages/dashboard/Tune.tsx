import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import React from "react";
import { connect } from "react-redux";
import { getTuneInfo } from "../../api";
import { StoreState } from "../../store";
import { dashboardActions } from "./state";
import Typography from "@material-ui/core/Typography";
import WarningRoundedIcon from "@material-ui/icons/WarningRounded";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(2)
      }
    },
    table: {
      marginTop: theme.spacing(1)
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1)
      }
    },
    warning: {
      fontSize: "0.8125rem",
      marginBottom: theme.spacing(2)
    },
    warningIcon: {
      fontSize: "1.25em",
      verticalAlign: "text-bottom"
    }
  });

const mapStateToProps = (state: StoreState) => ({
  tuneInfo: state.dashboard.tuneInfo
});

const mapDispatchToProps = dashboardActions;

class Tune extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps
> {
  timeout: number = 0;

  refreshTuneInfo = async () => {
    try {
      const [tuneInfo] = await Promise.all([getTuneInfo()]);
      this.props.setTuneInfo({ tuneInfo });
    } catch (error) {
      this.props.setError(error.toString());
    } finally {
      this.timeout = window.setTimeout(this.refreshTuneInfo, 1000);
    }
  };

  async componentDidMount() {
    await this.refreshTuneInfo();
  }

  async componentWillUnmount() {
    window.clearTimeout(this.timeout);
  }

  render() {
    const { classes, tuneInfo } = this.props;

    if (
      tuneInfo === null ||
      Object.keys(tuneInfo["trial_records"]).length === 0
    ) {
      return null;
    }

    const firstTrial = Object.keys(tuneInfo["trial_records"])[0];
    let paramNames: string[] = [];
    if (tuneInfo !== null) {
      const paramsDict = tuneInfo["trial_records"][firstTrial]["params"];
      paramNames = Object.keys(paramsDict).filter(k => k !== "args");
    }

    const metricNames = Object.keys(
      tuneInfo["trial_records"][firstTrial]["metrics"]
    );

    return (
      <div className={classes.root}>
        <Typography className={classes.warning} color="textSecondary">
          <WarningRoundedIcon className={classes.warningIcon} /> Note: This tab
          is experimental.
        </Typography>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              <TableCell className={classes.cell}>Trial ID</TableCell>
              <TableCell className={classes.cell}>Job ID</TableCell>
              <TableCell className={classes.cell}>Start Time</TableCell>
              {paramNames.map((value, index) => (
                <TableCell className={classes.cell} key={value}>
                  {value}
                </TableCell>
              ))}
              <TableCell className={classes.cell}>Status</TableCell>
              {metricNames.map((value, index) => (
                <TableCell className={classes.cell} key={value}>
                  {value}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {tuneInfo != null &&
              Object.keys(tuneInfo["trial_records"]).map(key => (
                <TableRow key={key}>
                  <TableCell className={classes.cell}>
                    {tuneInfo["trial_records"][key]["trial_id"]}
                  </TableCell>
                  <TableCell className={classes.cell}>
                    {tuneInfo["trial_records"][key]["job_id"]}
                  </TableCell>
                  <TableCell className={classes.cell}>
                    {tuneInfo["trial_records"][key]["start_time"]}
                  </TableCell>
                  {paramNames.map((value, index) => (
                    <TableCell className={classes.cell} key={value}>
                      {tuneInfo["trial_records"][key]["params"][value]}
                    </TableCell>
                  ))}
                  <TableCell className={classes.cell}>
                    {tuneInfo["trial_records"][key]["status"]}
                  </TableCell>
                  {tuneInfo["trial_records"][key]["metrics"] &&
                    metricNames.map((value, index) => (
                      <TableCell className={classes.cell} key={value}>
                        {tuneInfo["trial_records"][key]["metrics"][value]}
                      </TableCell>
                    ))}
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(withStyles(styles)(Tune));
