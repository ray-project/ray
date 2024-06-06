import { Tooltip } from "@mui/material";
import { styled } from "@mui/material/styles";
import { HelpInfo } from "../components/Tooltip";

export const TableContainerDiv = styled("div")(({theme}) => ({
  overflowX: "scroll",
}));

export const IdColTooltip = styled(Tooltip)(({theme}) => ({
  display: "block",
  width: "50px",
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
}));

export const IdColSpan = styled("span")(({theme}) => ({
  display: "block",
  width: "50px",
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
}));

export const OverflowColTooltip = styled(Tooltip)(({theme}) => ({
  display: "block",
  width: "100px",
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
}));

export const StyledHelpInfo = styled(HelpInfo)(({theme}) => ({
  marginLeft: theme.spacing(1),
}));