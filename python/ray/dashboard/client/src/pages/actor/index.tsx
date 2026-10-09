import { Box } from "@mui/material";
import React from "react";
import ActorList from "./ActorList";

/**
 * Represent the standalone actors page.
 */
const Actors = () => {
  return (
    <Box
      sx={(theme) => ({
        padding: 2,
        width: "100%",
        backgroundColor: theme.palette.background.paper,
      })}
    >
      <ActorList />
    </Box>
  );
};

export default Actors;
