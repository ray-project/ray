import { styled } from "@mui/material/styles";
import React from "react";
import ActorList from "./ActorList";

const ActorsRoot = styled("div")(({ theme }) => ({
  padding: theme.spacing(2),
  width: "100%",
  backgroundColor: "white",
}));

/**
 * Represent the standalone actors page.
 */
const Actors = () => {
  return (
    <ActorsRoot>
      <ActorList />
    </ActorsRoot>
  );
};

export default Actors;
