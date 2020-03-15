import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import React from "react";
import { connect } from "react-redux";
import { StoreState } from "../../../store";
import { dashboardActions } from "../state";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableHead from "@material-ui/core/TableHead";
import TableRow from "@material-ui/core/TableRow";
import NumberedLines from "../../../common/NumberedLines";
import Link from "@material-ui/core/Link";
import Dialog from "@material-ui/core/Dialog";
import DialogContent from "@material-ui/core/DialogContent";

const styles = (theme: Theme) =>
  createStyles({
    table: {
      marginTop: theme.spacing(1)
    },
    cell: {
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1)
      }
    }
  });

const mapStateToProps = (state: StoreState) => ({
  tuneInfo: state.dashboard.tuneInfo
});

const mapDispatchToProps = dashboardActions;

interface State {
  currentError: string;
  open: boolean;
}

class TuneErrors extends React.Component<
  WithStyles<typeof styles> &
    ReturnType<typeof mapStateToProps> &
    typeof mapDispatchToProps,
  State
> {
  state: State = {
    currentError: "",
    open: false
  };

  handleOpen = (key: string) => {
    this.setState({
      open: true,
      currentError: key
    });
  };

  handleClose = () => {
    this.setState({
      open: false
    });
  };

  render() {
    const { classes, tuneInfo } = this.props;
    const { currentError, open } = this.state;

    if (tuneInfo === null || Object.keys(tuneInfo["errors"]).length === 0) {
      return null;
    }

    return (
      <div>
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
            {tuneInfo["errors"] !== null &&
              Object.keys(tuneInfo["errors"]).map((key, index) => (
                <TableRow key={index}>
                  <TableCell className={classes.cell}>
                    {tuneInfo["errors"][key]["job_id"]}
                  </TableCell>
                  <TableCell className={classes.cell}>
                    {tuneInfo["errors"][key]["trial_id"]}
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
        <Dialog fullWidth maxWidth="md" open={open} onClose={this.handleClose}>
          <DialogContent>
            {open && (
              <NumberedLines
                lines={tuneInfo["errors"][currentError]["text"]
                  .trim()
                  .split("\n")}
              />
            )}
          </DialogContent>
        </Dialog>
      </div>
    );
  }
}

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(withStyles(styles)(TuneErrors));
