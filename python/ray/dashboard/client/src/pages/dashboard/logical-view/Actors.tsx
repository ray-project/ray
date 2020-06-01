import { createStyles, Theme, withStyles, WithStyles } from "@material-ui/core";
import React from "react";
import { RayletInfoResponse } from "../../../api";
import Actor from "./Actor";

const styles = (theme: Theme) => createStyles({});

type Props = {
  actors: RayletInfoResponse["actors"];
};

class Actors extends React.Component<Props & WithStyles<typeof styles>> {
  render() {
    const { actors } = this.props;
    return Object.entries(actors).map(([actorId, actor]) => (
      <Actor actor={actor} key={actorId} />
    ));
  }
}

export default withStyles(styles)(Actors);
