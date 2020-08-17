import {
  FormControl,
  FormHelperText,
  Input,
  InputLabel,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { connect } from "react-redux";
import { ActorInfo, isFullActorInfo, RayletInfoResponse } from "../../../api";
import { filterObj } from "../../../common/util";
import { StoreState } from "../../../store";
import ActorClassGroups from "./ActorClassGroups";

const actorMatchesSearch = (actor: ActorInfo, nameFilter: string): boolean => {
  // Performs a case insensitive search for the name filter string within the
  // actor and all of its nested subactors.
  const actorTitles = getNestedActorTitles(actor);
  const loweredNameFilter = nameFilter.toLowerCase();
  const match = actorTitles.find(
    (actorTitle) => actorTitle.toLowerCase().search(loweredNameFilter) !== -1,
  );
  return match !== undefined;
};

const getNestedActorTitles = (actor: ActorInfo): string[] => {
  const actorTitle = actor.actorTitle;
  const titles: string[] = actorTitle ? [actorTitle] : [];
  if (!isFullActorInfo(actor)) {
    return titles;
  }
  const children = actor["children"];
  if (children === undefined || Object.entries(children).length === 0) {
    return titles;
  }
  const childrenTitles = Object.values(children).flatMap((actor) =>
    getNestedActorTitles(actor),
  );
  return titles.concat(childrenTitles);
};

const mapStateToProps = (state: StoreState) => ({
  rayletInfo: state.dashboard.rayletInfo,
});

type LogicalViewProps = {
  rayletInfo: RayletInfoResponse | null;
} & ReturnType<typeof mapStateToProps>;

const LogicalView: React.FC<LogicalViewProps> = ({ rayletInfo }) => {
  const [nameFilter, setNameFilter] = useState("");

  if (rayletInfo === null) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }
  let filteredActors = rayletInfo.actors;
  if (nameFilter !== "") {
    filteredActors = filterObj(filteredActors, ([_, actor]: [any, ActorInfo]) =>
      actorMatchesSearch(actor, nameFilter),
    );
  }

  return (
    <div>
      {Object.entries(rayletInfo.actors).length === 0 ? (
        <Typography color="textSecondary">No actors found.</Typography>
      ) : (
        <div>
          <FormControl>
            <InputLabel htmlFor="actor-name-filter">Actor Search</InputLabel>
            <Input
              id="actor-name-filter"
              aria-describedby="actor-name-helper-text"
              value={nameFilter}
              onChange={(event) => setNameFilter(event.target.value)}
            />
            <FormHelperText id="actor-name-helper-text">
              Search for an actor by name
            </FormHelperText>
          </FormControl>
          <ActorClassGroups actors={Object.values(filteredActors)} />
        </div>
      )}
    </div>
  );
};

export default connect(mapStateToProps)(LogicalView);
