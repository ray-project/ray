import { Box, makeStyles, Typography } from "@material-ui/core";
import React from "react";

type ExpanderRowProps = {
  onExpand: () => any;
};

const useExpanderRowStyles = makeStyles({
  root: {
    cursor: "pointer",
  },
});

const ExpanderRow: React.FC<ExpanderRowProps> = ({ onExpand }) => {
  const classes = useExpanderRowStyles();
  return (
    <Box onClick={(_) => onExpand()} component="div" className={classes.root}>
      <Typography variant="overline">Show more</Typography>
    </Box>
  );
};

export default ExpanderRow;
