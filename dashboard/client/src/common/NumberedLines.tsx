import { Table, TableBody, TableCell, TableRow, Theme } from "@mui/material";
import { styled } from "@mui/material/styles";
import React from "react";

const BaseTableCell = styled(TableCell)(({ theme }) => ({
  borderWidth: 0,
  fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
  padding: 0,
  "&:last-child": {
    paddingRight: 0,
  },
}));

const LineNumberCell = styled(BaseTableCell)(({ theme }) => ({
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
}));

const LineCell = styled(BaseTableCell)(({ theme }) => ({
  textAlign: "left",
  whiteSpace: "pre-wrap",
}));

type Props = {
  lines: string[];
};

class NumberedLines extends React.Component<Props> {
  render() {
    const { lines } = this.props;
    return (
      <Table>
        <TableBody>
          {lines.map((line, index) => (
            <TableRow key={index}>
              <LineNumberCell data-line-number={index + 1} />
              <LineCell>{line}</LineCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    );
  }
}

export default NumberedLines;
