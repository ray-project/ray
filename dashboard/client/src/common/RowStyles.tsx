import { createStyles, makeStyles } from "@material-ui/core/styles";

const rowStyles = makeStyles((theme) =>
  createStyles({
    expandCollapseIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
    idCol: {
      display: "block",
      width: "50px",
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    },
  }),
);

export default rowStyles;
