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
    cell: {
      borderWidth: 0,
      fontFamily: [
        "Consolas",
        "Andale Mono WT",
        "Andale Mono",
        "Lucida Console",
        "Lucida Sans Typewriter",
        "DejaVu Sans Mono",
        "Bitstream Vera Sans Mono",
        "Liberation Mono",
        "Nimbus Mono L",
        "Monaco",
        "Courier New",
        "Courier",
        "monospace"
      ]
        .map(font => `"${font}"`)
        .join(","),
      padding: 0,
      "&:last-child": {
        paddingRight: 0
      }
    },
    number: {
      color: theme.palette.text.secondary,
      textAlign: "right",
      userSelect: "none",
      width: "1%"
    },
    line: {
      paddingLeft: theme.spacing(2),
      textAlign: "left",
      whiteSpace: "pre"
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
              <TableCell className={classNames(classes.cell, classes.number)}>
                {index + 1}
              </TableCell>
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
