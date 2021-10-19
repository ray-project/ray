import { makeStyles } from "@material-ui/core";
import React, { PropsWithChildren } from "react";

const useStyle = makeStyles((theme) => ({
  container: {
    background: "linear-gradient(45deg, #21CBF3ee 30%, #2196F3ee 90%)",
    border: `1px solid #ffffffbb`,
    padding: "0 12px",
    height: 18,
    lineHeight: "18px",
    position: "relative",
    boxSizing: "content-box",
    borderRadius: 4,
  },
  displayBar: {
    background: theme.palette.background.paper,
    position: "absolute",
    right: 0,
    height: 18,
    transition: "0.5s width",
    borderRadius: 2,
    borderTopLeftRadius: 0,
    borderBottomLeftRadius: 0,
    border: "2px solid transparent",
    boxSizing: "border-box",
  },
  text: {
    fontSize: 12,
    zIndex: 2,
    position: "relative",
    color: theme.palette.text.primary,
    width: "100%",
    textAlign: "center",
  },
}));

const PercentageBar = (
  props: PropsWithChildren<{ num: number; total: number }>,
) => {
  const { num, total } = props;
  const classes = useStyle();
  const per = Math.round((num / total) * 100);

  return (
    <div className={classes.container}>
      <div
        className={classes.displayBar}
        style={{
          width: `${Math.min(Math.max(0, 100 - per), 100)}%`,
        }}
      />
      <div className={classes.text}>{props.children}</div>
    </div>
  );
};

export default PercentageBar;
