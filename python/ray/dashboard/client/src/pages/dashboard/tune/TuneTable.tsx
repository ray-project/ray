import {
  createStyles,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TableSortLabel,
  Theme,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import { TuneTrial } from "../../../api";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      padding: theme.spacing(2),
      "& > :not(:first-child)": {
        marginTop: theme.spacing(2),
      },
    },
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "right",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tuneInfo: state.dashboard.tuneInfo,
});

type State = {
  metricParamColumn: string;
  ascending: boolean;
  sortedColumn: keyof TuneTrial | undefined;
};

const mapDispatchToProps = dashboardActions;

class TuneTable extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  timeout: number = 0;

  state: State = {
    sortedColumn: undefined,
    ascending: true,
    metricParamColumn: "",
  };

  onColumnClick = (column: keyof TuneTrial, metricParamColumn?: string) => {
    let ascending = this.state.ascending;
    if (column === this.state.sortedColumn) {
      ascending = !ascending;
    } else {
      ascending = true;
    }
    this.setState({
      sortedColumn: column,
      ascending: ascending,
    });

    if (metricParamColumn) {
      this.setState({
        metricParamColumn: metricParamColumn,
      });
    }
  };

  /**
   * Replaces all underscores with spaces and capitalizes all words
   * in str
   */
  humanize = (str: string) =>
    str
      .split("_")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");

  sortedCell = (name: keyof TuneTrial, chosenMetricParam?: string) => {
    const { tuneInfo, classes } = this.props;
    const { sortedColumn, ascending, metricParamColumn } = this.state;
    let label: "desc" | "asc" = "asc";

    if (name === sortedColumn && !ascending) {
      label = "desc";
    }

    if (tuneInfo === null) {
      return;
    }

    let onClick = () => this.onColumnClick(name);
    if (chosenMetricParam) {
      onClick = () => this.onColumnClick(name, chosenMetricParam);
    }

    let active = false;
    let key: string = name;
    if (chosenMetricParam) {
      key = chosenMetricParam;
      active = chosenMetricParam === metricParamColumn && sortedColumn === name;
    } else {
      active = name === sortedColumn;
    }

    return (
      <TableCell className={classes.cell} key={key} onClick={onClick}>
        <TableSortLabel active={active} direction={label} />
        {chosenMetricParam
          ? this.humanize(chosenMetricParam)
          : this.humanize(name)}
      </TableCell>
    );
  };

  sortedTrialRecords = () => {
    const { tuneInfo } = this.props;
    const { sortedColumn, ascending, metricParamColumn } = this.state;

    if (
      tuneInfo === null ||
      Object.keys(tuneInfo["trial_records"]).length === 0
    ) {
      return null;
    }

    const trialDetails = Object.values(tuneInfo["trial_records"]);

    if (!sortedColumn) {
      return trialDetails;
    }

    let getAttribute = (trial: TuneTrial) => trial[sortedColumn!];
    if (sortedColumn === "metrics" || sortedColumn === "params") {
      getAttribute = (trial: TuneTrial) =>
        trial[sortedColumn!][metricParamColumn];
    }

    if (sortedColumn) {
      if (ascending) {
        trialDetails.sort((a, b) =>
          getAttribute(a) > getAttribute(b) ? 1 : -1,
        );
      } else if (!ascending) {
        trialDetails.sort((a, b) =>
          getAttribute(a) < getAttribute(b) ? 1 : -1,
        );
      }
    }

    return trialDetails;
  };

  render() {
    const { classes, tuneInfo } = this.props;

    if (
      tuneInfo === null ||
      Object.keys(tuneInfo["trial_records"]).length === 0
    ) {
      return null;
    }

    const firstTrial = Object.keys(tuneInfo["trial_records"])[0];
    const paramsDict = tuneInfo["trial_records"][firstTrial]["params"];
    const paramNames = Object.keys(paramsDict).filter((k) => k !== "args");

    const metricNames = Object.keys(
      tuneInfo["trial_records"][firstTrial]["metrics"],
    );

    const trialDetails = this.sortedTrialRecords();

    return (
      <div className={classes.root}>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              {this.sortedCell("trial_id")}
              {this.sortedCell("job_id")}
              {this.sortedCell("start_time")}
              {paramNames.map((value) => this.sortedCell("params", value))}
              {this.sortedCell("status")}
              {metricNames.map((value) => this.sortedCell("metrics", value))}
            </TableRow>
          </TableHead>
          <TableBody>
            {trialDetails !== null &&
              trialDetails.map((trial, index) => (
                <TableRow key={index}>
                  <TableCell className={classes.cell}>
                    {trial["trial_id"]}
                  </TableCell>
                  <TableCell className={classes.cell}>
                    {trial["job_id"]}
                  </TableCell>
                  <TableCell className={classes.cell}>
                    {trial["start_time"]}
                  </TableCell>
                  {paramNames.map((value) => (
                    <TableCell className={classes.cell} key={value}>
                      {trial["params"][value]}
                    </TableCell>
                  ))}
                  <TableCell className={classes.cell}>
                    {trial["status"]}
                  </TableCell>
                  {trial["metrics"] &&
                    metricNames.map((value) => (
                      <TableCell className={classes.cell} key={value}>
                        {trial["metrics"][value]}
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
  mapDispatchToProps,
)(withStyles(styles)(TuneTable));
