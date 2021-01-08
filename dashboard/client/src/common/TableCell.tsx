import { TableCell } from "@material-ui/core";
import { styled } from "@material-ui/core/styles";

export const StyledTableCell = styled(TableCell)(({ theme }) => ({
  padding: theme.spacing(1),
  textAlign: "center",
}));

export const ExpandableStyledTableCell = styled(TableCell)(({ theme }) => ({
  padding: theme.spacing(1),
  textAlign: "center",
  cursor: "pointer",
}));
