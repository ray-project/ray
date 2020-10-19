import {
  createStyles,
  Link,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Theme,
  withStyles,
  WithStyles,
} from "@material-ui/core";
import React from "react";
import { connect } from "react-redux";
import DialogWithTitle from "../../../common/DialogWithTitle";
import NumberedLines from "../../../common/NumberedLines";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1),
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
  });

const mapStateToProps = (state: StoreState) => ({
  tuneInfo: state.dashboard.tuneInfo,
});

const mapDispatchToProps = dashboardActions;

type State = {
  currentError: string;
  open: boolean;
};

class TuneErrors extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  state: State = {
    currentError: "",
    open: false,
  };

  handleOpen = (key: string) => {
    this.setState({
      open: true,
      currentError: key,
    });
  };

  handleClose = () => {
    this.setState({
      open: false,
    });
  };

  render() {
    const { classes, tuneInfo } = this.props;
    const { currentError, open } = this.state;

    if (tuneInfo === null || Object.keys(tuneInfo.errors).length === 0) {
      return null;
    }

    return (
      <React.Fragment>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              <TableCell className={classes.cell}> Job ID</TableCell>
              <TableCell className={classes.cell}> Trial ID </TableCell>
              <TableCell className={classes.cell}> Trial Directory </TableCell>
              <TableCell className={classes.cell}> Error </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tuneInfo.errors !== null &&
              Object.keys(tuneInfo.errors).map((key, index) => (
                <TableRow key={index}>
                  <TableCell className={classes.cell}>
                    {tuneInfo.errors[key].jobId}
                  </TableCell>
                  <TableCell className={classes.cell}>
                    {tuneInfo.errors[key].trialId}
                  </TableCell>
                  <TableCell className={classes.cell}>{key}</TableCell>
                  <TableCell className={classes.cell}>
                    <Link
                      component="button"
                      variant="body2"
                      onClick={() => {
                        this.handleOpen(key);
                      }}
                    >
                      Show Error
                    </Link>
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
        {open && (
          <DialogWithTitle handleClose={this.handleClose} title="Error Log">
            {open && (
              <NumberedLines
                lines={tuneInfo.errors[currentError].text.trim().split("\n")}
              />
            )}
          </DialogWithTitle>
        )}
      </React.Fragment>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(withStyles(styles)(TuneErrors));
