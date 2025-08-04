import { styled, Table, TableBody, TableCell, TableRow } from "@mui/material";
import React from "react";

const StyledTableCell = styled(TableCell)(() => ({
  borderWidth: 0,
  fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
  padding: 0,
  "&:last-child": {
    paddingRight: 0,
  },
}));

type NumberedLinesProps = {
  lines: string[];
};

const NumberedLines = ({ lines }: NumberedLinesProps) => {
  return (
    <Table>
      <TableBody>
        {lines.map((line, index) => (
          <TableRow key={index}>
            <StyledTableCell
              sx={(theme) => ({
                color: theme.palette.text.secondary,
                paddingRight: 2,
                textAlign: "right",
                verticalAlign: "top",
                width: "1%",
                // Use a ::before pseudo-element for the line number so that it won't
                // interact with user selections or searching.
                "&::before": {
                  content: "attr(data-line-number)",
                },
              })}
              data-line-number={index + 1}
            />
            <StyledTableCell sx={{ textAlign: "left", whiteSpace: "pre-wrap" }}>
              {line}
            </StyledTableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
};

export default NumberedLines;
