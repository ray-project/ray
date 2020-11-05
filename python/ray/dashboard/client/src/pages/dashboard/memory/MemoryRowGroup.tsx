import {
  Box,
  createStyles,
  makeStyles,
  Paper,
  styled,
  Theme,
} from "@material-ui/core";
import React, { ReactChild, useState } from "react";
import { MemoryTableEntry, MemoryTableSummary } from "../../../api";
import { Expander, Minimizer } from "../../../common/ExpandControls";
import MemorySummary from "./MemorySummary";
import MemoryTable from "./MemoryTable";

const CenteredBox = styled(Box)({
  textAlign: "center",
});

const useMemoryRowGroupStyles = makeStyles((theme: Theme) =>
  createStyles({
    container: {
      marginTop: theme.spacing(2),
      marginBottom: theme.spacing(2),
      paddingTop: theme.spacing(2),
      paddingBottom: theme.spacing(1),
      paddingLeft: theme.spacing(2),
      paddingRight: theme.spacing(2),
    },
  }),
);

type MemoryRowGroupProps = {
  groupKey: string;
  groupTitle: ReactChild;
  summary: MemoryTableSummary;
  entries: MemoryTableEntry[];
  initialExpanded: boolean;
  initialVisibleEntries: number;
};

const MemoryRowGroup: React.FC<MemoryRowGroupProps> = ({
  groupKey,
  groupTitle,
  entries,
  summary,
  initialExpanded,
  initialVisibleEntries,
}) => {
  const classes = useMemoryRowGroupStyles();
  const [expanded, setExpanded] = useState(initialExpanded);
  const [numVisibleEntries, setNumVisibleEntries] = useState(
    initialVisibleEntries,
  );
  const toggleExpanded = () => setExpanded(!expanded);
  const showMoreEntries = () => setNumVisibleEntries(numVisibleEntries + 10);
  const visibleEntries = entries.slice(0, numVisibleEntries);
  return (
    <Paper key={groupKey} className={classes.container}>
      {groupTitle}
      <MemorySummary initialExpanded={false} memoryTableSummary={summary} />
      {expanded ? (
        <React.Fragment>
          <MemoryTable tableEntries={visibleEntries} />
          <CenteredBox>
            {entries.length > numVisibleEntries && (
              <Expander onClick={showMoreEntries} />
            )}
            <Minimizer onClick={toggleExpanded} />
          </CenteredBox>
        </React.Fragment>
      ) : (
        <CenteredBox>
          <Expander onClick={toggleExpanded} />
        </CenteredBox>
      )}
    </Paper>
  );
};

export default MemoryRowGroup;
