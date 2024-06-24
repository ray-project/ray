import { Box, Theme, useTheme } from "@mui/material";
import React from "react";
import ActorList from "./ActorList";

const useStyles = (theme: Theme) => ({
  root: {
    padding: theme.spacing(2),
    width: "100%",
    backgroundColor: "white",
  },
});

/**
 * Represent the standalone actors page.
 */
const Actors = () => {
  const styles = useStyles(useTheme());

  return (
    <Box sx={styles.root}>
      <ActorList />
    </Box>
  );
};

export default Actors;
