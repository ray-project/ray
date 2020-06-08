import {
  Typography
} from "@material-ui/core";
import React, { useState } from "react";
import { connect } from "react-redux";
import { StoreState } from "../../../store";
import Actors from "./Actors"
import { RayletInfoResponse, RayletActorInfo } from "../../../api";

const mapStateToProps = (state: StoreState) => ({
  rayletInfo: state.dashboard.rayletInfo,
});

const filterObj = (obj: Object, filterFn: any) => Object.fromEntries(Object.entries(obj).filter(filterFn))

type LogicalViewProps = {
  rayletInfo: RayletInfoResponse | null;
} & ReturnType<typeof mapStateToProps>;

const LogicalView = (props: LogicalViewProps) => {
  const { rayletInfo } = props;
  const [nameFilter, setNameFilter] = useState<string | null>(null);
  const filterActors = (actors: RayletActorInfo[], nameFilter: string) =>
      actors.filter(actor => actor.actorTitle.search(nameFilter) !== -1);
  if (rayletInfo === null) {
    return <Typography color="textSecondary">Loading...</Typography>
  }
  let filteredActors = rayletInfo.actors;
  if (nameFilter !== null) {
    filteredActors = filterObj(filteredActors, ([_, actor]: [any, RayletActorInfo]) => actor.actorTitle.search(nameFilter) !== -1)
  }
  return (
    <div>
      {
      Object.entries(rayletInfo.actors).length === 0 ? (
        <Typography color="textSecondary">No actors found.</Typography>
      ) : (
        <Actors actors={filteredActors} />
      )}
    </div>
  );
}

export default connect(mapStateToProps)(LogicalView);
