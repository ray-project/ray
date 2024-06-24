import {
  Table,
  TableBody,
  TableCell,
  TableRow,
  Theme,
  useTheme,
} from "@mui/material";
import React from "react";

const useStyles = (theme: Theme) => ({
  root: {
    overflowX: "auto",
  },
  cell: {
    borderWidth: 0,
    fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
    padding: 0,
    "&:last-child": {
      paddingRight: 0,
    },
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
      content: "attr(data-line-number)",
    },
  },
  line: {
    textAlign: "left",
    whiteSpace: "pre-wrap",
  },
});

type NumberedLinesProps = {
  lines: string[];
};

const NumberedLines = ({ lines }: NumberedLinesProps) => {
  const styles = useStyles(useTheme());
  return (
    <Table>
      <TableBody>
        {lines.map((line, index) => (
          <TableRow key={index}>
            <TableCell
              sx={Object.assign({}, styles.cell, styles.lineNumber)}
              data-line-number={index + 1}
            />
            <TableCell sx={Object.assign({}, styles.cell, styles.line)}>
              {line}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
};

export default NumberedLines;
