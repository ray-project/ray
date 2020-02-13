import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import React, { HTMLAttributes } from "react";

const styles = (theme: Theme) =>
  createStyles({
    button: {
      color: theme.palette.primary.main,
      "&:hover": {
        cursor: "pointer",
        textDecoration: "underline"
      }
    }
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
