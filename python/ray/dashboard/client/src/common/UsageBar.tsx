import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import React from "react";

const blend = (
  [r1, g1, b1]: number[],
  [r2, g2, b2]: number[],
  ratio: number
) => [
  r1 * (1 - ratio) + r2 * ratio,
  g1 * (1 - ratio) + g2 * ratio,
  b1 * (1 - ratio) + b2 * ratio
];

const styles = (theme: Theme) =>
  createStyles({
    root: {
      borderColor: theme.palette.divider,
      borderStyle: "solid",
      borderWidth: 1
    }
  });

interface Props {
  percent: number;
  text: string;
}

class Component extends React.Component<Props & WithStyles<typeof styles>> {
  render() {
    const { classes, text } = this.props;

    let { percent } = this.props;
    percent = Math.max(percent, 0);
    percent = Math.min(percent, 100);

    const minColor = [0, 255, 0];
    const maxColor = [255, 0, 0];

    const leftColor = minColor;
    const rightColor = blend(minColor, maxColor, percent / 100);
    const alpha = 0.2;

    const gradient = `
      linear-gradient(
        to right,
        rgba(${leftColor.join(",")}, ${alpha}) 0%,
        rgba(${rightColor.join(",")}, ${alpha}) ${percent}%,
        transparent ${percent}%
      )
    `;

    // Use a nested `div` here because the right border is affected by the
    // gradient background otherwise.
    return (
      <div className={classes.root}>
        <div style={{ background: gradient }}>{text}</div>
      </div>
    );
  }
}

export default withStyles(styles)(Component);
