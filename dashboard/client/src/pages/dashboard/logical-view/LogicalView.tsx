import {
  Box,
  createStyles,
  FormControl,
  FormHelperText,
  Input,
  InputLabel,
  makeStyles,
  Theme,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import { useSelector } from "react-redux";
import { useDebounce } from "use-debounce";
import { StoreState } from "../../../store";
import ActorClassGroups from "./ActorClassGroups";

const useLogicalViewStyles = makeStyles((theme: Theme) =>
  createStyles({
    container: {
      marginBottom: theme.spacing(1),
    },
  }),
);

const actorClassMatchesSearch = (
  actorClass: string,
  nameFilter: string,
): boolean => {
  const loweredNameFilter = nameFilter.toLowerCase();
  return actorClass.toLowerCase().search(loweredNameFilter) !== -1;
};

const actorGroupsSelector = (state: StoreState) => state.dashboard.actorGroups;

const LogicalView: React.FC = () => {
  const [nameFilter, setNameFilter] = useState("");
  const [debouncedNameFilter] = useDebounce(nameFilter, 500);
  const classes = useLogicalViewStyles();
  const actorGroups = useSelector(actorGroupsSelector);
  if (!actorGroups) {
    return <Typography color="textSecondary">Loading...</Typography>;
  }
  if (Object.keys(actorGroups).length === 0) {
    return (
      <Typography color="textSecondary">
        Finished loading, but have found no actors yet.
      </Typography>
    );
  }
  const filteredGroups =
    debouncedNameFilter === ""
      ? Object.entries(actorGroups)
      : Object.entries(actorGroups).filter(([key, _]) =>
          actorClassMatchesSearch(key, debouncedNameFilter),
        );
  return (
    <Box className={classes.container}>
      {filteredGroups.length === 0 ? (
        <Typography color="textSecondary">No actors found.</Typography>
      ) : (
        <React.Fragment>
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
          <ActorClassGroups actorGroups={Object.fromEntries(filteredGroups)} />
        </React.Fragment>
      )}
    </Box>
  );
};

export default LogicalView;
