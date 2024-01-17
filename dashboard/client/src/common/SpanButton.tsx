import { Theme } from "@mui/material";
import { WithStyles } from "@mui/styles";
import createStyles from "@mui/styles/createStyles";
import withStyles from "@mui/styles/withStyles";
import React, { HTMLAttributes } from "react";

const styles = (theme: Theme) =>
  createStyles({
    button: {
      color: theme.palette.primary.main,
      "&:hover": {
        cursor: "pointer",
        textDecoration: "underline",
      },
    },
  });

class SpanButton extends React.Component<
  HTMLAttributes<HTMLSpanElement> & WithStyles<typeof styles>
> {
  render() {
    const { classes, ...otherProps } = this.props;
    return <span className={classes.button} {...otherProps} />;
  }
}

export default withStyles(styles)(SpanButton);
