import { Box } from "@mui/material";
import React from "react";
import ActorList from "./ActorList";

/**
 * Represent the standalone actors page.
 */
const Actors = () => {
  return (
    <Box
      sx={{
        padding: 2,
        width: "100%",
        backgroundColor: "white",
      }}
    >
      <ActorList />
    </Box>
  );
};

export default Actors;
