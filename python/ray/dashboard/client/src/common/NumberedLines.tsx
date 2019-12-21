import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Table from "@material-ui/core/Table";
import TableBody from "@material-ui/core/TableBody";
import TableCell from "@material-ui/core/TableCell";
import TableRow from "@material-ui/core/TableRow";
import classNames from "classnames";
import React from "react";

const styles = (theme: Theme) =>
  createStyles({
    root: {
      overflowX: "auto"
    },
    cell: {
      borderWidth: 0,
      fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
      padding: 0,
      "&:last-child": {
        paddingRight: 0
      }
    },
    lineNumber: {
      color: theme.palette.text.secondary,
      paddingRight: theme.spacing(2),
      textAlign: "right",
      verticalAlign: "top",
      width: "1%",
      // Use a ::before pseudo-element for the line number so that it won't
      // interact with user selections or searching.
      "&::before": {
        content: "attr(data-line-number)"
      }
    },
    line: {
      textAlign: "left",
      whiteSpace: "pre-wrap"
    }
  });

interface Props {
  lines: string[];
}

class Component extends React.Component<Props & WithStyles<typeof styles>> {
  render() {
    const { classes, lines } = this.props;
    return (
      <Table>
        <TableBody>
          {lines.map((line, index) => (
            <TableRow key={index}>
              <TableCell
                className={classNames(classes.cell, classes.lineNumber)}
                data-line-number={index + 1}
              />
              <TableCell className={classNames(classes.cell, classes.line)}>
                {line}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  }
}

export default withStyles(styles)(Component);
