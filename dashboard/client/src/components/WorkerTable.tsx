import {
  Button,
  Grid,
  IconButton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from "@material-ui/core";
import { KeyboardArrowDown, KeyboardArrowRight } from "@material-ui/icons";
import React, {
  PropsWithChildren,
  ReactNode,
  useContext,
  useEffect,
  useState,
} from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../App";
import { formatDateFromTimeMs } from "../common/formatUtils";
import { Actor } from "../type/actor";
import { CoreWorkerStats, Worker } from "../type/worker";
import { memoryConverter } from "../util/converter";
import { longTextCut } from "../util/func";

import { useFilter } from "../util/hook";
import ActorTable from "./ActorTable";
import PercentageBar from "./PercentageBar";
import { SearchInput } from "./SearchComponent";

export const ExpandableTableRow = ({
  children,
  expandComponent,
  length,
  stateKey = "",
  ...otherProps
}: PropsWithChildren<{
  expandComponent: ReactNode;
  length: number;
  stateKey?: string;
}>) => {
  const [isExpanded, setIsExpanded] = React.useState(false);

  useEffect(() => {
    if (stateKey.startsWith("ON")) {
      setIsExpanded(true);
    } else if (stateKey.startsWith("OFF")) {
      setIsExpanded(false);
    }
  }, [stateKey]);

  if (length < 1) {
    return (
      <TableRow {...otherProps}>
        <TableCell padding="checkbox" />
        {children}
      </TableRow>
    );
  }

  return (
    <React.Fragment>
      <TableRow {...otherProps}>
        <TableCell padding="checkbox">
          <IconButton
            style={{ color: "inherit" }}
            onClick={() => setIsExpanded(!isExpanded)}
          >
            {length}
            {isExpanded ? <KeyboardArrowDown /> : <KeyboardArrowRight />}
          </IconButton>
        </TableCell>
        {children}
      </TableRow>
      {isExpanded && (
        <TableRow>
          <TableCell colSpan={24}>{expandComponent}</TableCell>
        </TableRow>
      )}
    </React.Fragment>
  );
};

const WorkerDetailTable = ({
  actorMap,
  coreWorkerStats,
}: {
  actorMap: { [actorId: string]: Actor };
  coreWorkerStats: CoreWorkerStats[];
}) => {
  const actors = {} as { [actorId: string]: Actor };
  (coreWorkerStats || [])
    .filter((e) => actorMap[e.actorId])
    .forEach((e) => (actors[e.actorId] = actorMap[e.actorId]));

  if (!Object.values(actors).length) {
    return <p>The Worker Haven't Had Related Actor Yet.</p>;
  }

  return (
    <TableContainer>
      <ActorTable actors={actors} detailPathPrefix="/actors" />
    </TableContainer>
  );
};

const RayletWorkerTable = ({
  workers = [],
  actorMap,
  mini,
}: {
  workers: Worker[];
  actorMap: { [actorId: string]: Actor };
  mini?: boolean;
}) => {
  const { changeFilter, filterFunc } = useFilter();
  const [key, setKey] = useState("");
  const { nodeMapByIp } = useContext(GlobalContext);
  const open = () => setKey(`ON${Math.random()}`);
  const close = () => setKey(`OFF${Math.random()}`);

  return (
    <React.Fragment>
      {!mini && (
        <div style={{ display: "flex", alignItems: "center" }}>
          <SearchInput
            label="Pid"
            onChange={(value) => changeFilter("pid", value)}
          />
          <Button onClick={open}>Expand All</Button>
          <Button onClick={close}>Collapse All</Button>
        </div>
      )}{" "}
      <Table>
        <TableHead>
          <TableRow>
            {[
              "",
              "Pid",
              "CPU",
              "CPU Times",
              "Memory",
              "CMD Line",
              "Create Time",
              "Log",
              "Ops",
              "IP",
              "Tasks",
              "Objects",
            ].map((col) => (
              <TableCell align="center" key={col}>
                {col}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {workers
            .filter(filterFunc)
            .sort((aWorker, bWorker) => {
              const a =
                (aWorker.coreWorkerStats || []).filter(
                  (e) => actorMap[e.actorId],
                ).length || 0;
              const b =
                (bWorker.coreWorkerStats || []).filter(
                  (e) => actorMap[e.actorId],
                ).length || 0;
              return b - a;
            })
            .map(
              ({
                pid,
                cpuPercent,
                cpuTimes,
                memoryInfo,
                cmdline,
                createTime,
                coreWorkerStats = [],
                language,
              }) => (
                <ExpandableTableRow
                  expandComponent={
                    <WorkerDetailTable
                      actorMap={actorMap}
                      coreWorkerStats={coreWorkerStats}
                    />
                  }
                  length={
                    (coreWorkerStats || []).filter((e) => actorMap[e.actorId])
                      .length
                  }
                  key={pid}
                  stateKey={key}
                >
                  <TableCell align="center">{pid}</TableCell>
                  <TableCell align="center">
                    <PercentageBar num={Number(cpuPercent)} total={100}>
                      {cpuPercent}%
                    </PercentageBar>
                  </TableCell>
                  <TableCell align="center">
                    <div style={{ maxHeight: 55, overflow: "auto" }}>
                      {Object.entries(cpuTimes || {}).map(([key, val]) => (
                        <div style={{ margin: 4 }}>
                          {key}:{val}%
                        </div>
                      ))}
                    </div>
                  </TableCell>
                  <TableCell align="center">
                    <div style={{ maxHeight: 55, overflow: "auto" }}>
                      {Object.entries(memoryInfo || {}).map(([key, val]) => (
                        <div style={{ margin: 4 }}>
                          {key}:{memoryConverter(val)}
                        </div>
                      ))}
                    </div>
                  </TableCell>
                  <TableCell align="center" style={{ lineBreak: "anywhere" }}>
                    {cmdline && longTextCut(cmdline.filter((e) => e).join(" "))}
                  </TableCell>
                  <TableCell align="center">
                    {formatDateFromTimeMs(createTime * 1000)}
                  </TableCell>
                  <TableCell align="center">
                    <Grid container spacing={2}>
                      {coreWorkerStats[0] && (
                        <Grid item>
                          <Link
                            target="_blank"
                            to={`/logs/?nodeId=${encodeURIComponent(
                              nodeMapByIp[coreWorkerStats[0].ipAddress],
                            )}&fileName=${
                              coreWorkerStats[0].jobId || ""
                            }-${pid}`}
                          >
                            Log
                          </Link>
                        </Grid>
                      )}
                    </Grid>
                  </TableCell>
                  <TableCell align="center">
                    {language === "JAVA" ? (
                      <div>
                        <Button
                          onClick={() => {
                            window.open(
                              `#/cmd/jstack/${coreWorkerStats[0]?.ipAddress}/${pid}`,
                            );
                          }}
                        >
                          jstack
                        </Button>{" "}
                        <Button
                          onClick={() => {
                            window.open(
                              `#/cmd/jmap/${coreWorkerStats[0]?.ipAddress}/${pid}`,
                            );
                          }}
                        >
                          jmap
                        </Button>
                        <Button
                          onClick={() => {
                            window.open(
                              `#/cmd/jstat/${coreWorkerStats[0]?.ipAddress}/${pid}`,
                            );
                          }}
                        >
                          jstat
                        </Button>
                      </div>
                    ) : (
                      "N/A"
                    )}
                  </TableCell>
                  <TableCell align="center">
                    {nodeMapByIp[coreWorkerStats[0]?.ipAddress] ? (
                      <Link
                        target="_blank"
                        to={`/cluster/nodes/${
                          nodeMapByIp[coreWorkerStats[0]?.ipAddress]
                        }`}
                      >
                        {coreWorkerStats[0]?.ipAddress}
                      </Link>
                    ) : (
                      coreWorkerStats[0]?.ipAddress
                    )}
                  </TableCell>
                  <TableCell align="center">
                    <div>
                      Pending tasks: {coreWorkerStats[0]?.numPendingTasks}
                    </div>
                    <div>
                      Executed tasks: {coreWorkerStats[0]?.numExecutedTasks}
                    </div>
                  </TableCell>
                  <TableCell align="center">
                    <div>
                      ObjectRefs in scope:{" "}
                      {coreWorkerStats[0]?.numObjectRefsInScope}
                    </div>
                    <div>
                      Objects in local memory store:{" "}
                      {coreWorkerStats[0]?.numLocalObjects}
                    </div>
                    <div>
                      Objects in plasma store: {coreWorkerStats[0]?.numInPlasma}
                    </div>
                    <div>
                      Object store Memory used (MiB):{" "}
                      {coreWorkerStats[0]?.usedObjectStoreMemory}
                    </div>
                  </TableCell>
                </ExpandableTableRow>
              ),
            )}
        </TableBody>
      </Table>
    </React.Fragment>
  );
};

export default RayletWorkerTable;
