import {
  Button,
  IconButton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
} from "@material-ui/core";
import { KeyboardArrowDown, KeyboardArrowRight } from "@material-ui/icons";
import { get } from "lodash";
import moment from "moment";
import numeral from "numeral";
import React, {
  PropsWithChildren,
  ReactNode,
  useEffect,
  useState,
} from "react";
import { Actor } from "../type/actor";
import { CoreWorkerStats, Worker } from "../type/worker";
import ActorTable from "./ActorTable";
import PercentageBar from "./PercentageBar";
import { SearchInput } from "./SearchComponent";

export const longTextCut = (text: string = "", len: number = 28) => (
  <Tooltip title={text} interactive>
    <span>{text.length > len ? text.slice(0, len) + "..." : text}</span>
  </Tooltip>
);

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
      <ActorTable actors={actors} />
    </TableContainer>
  );
};

export const useFilter = <KeyType extends string>() => {
  const [filters, setFilters] = useState<{ key: KeyType; val: string }[]>([]);
  const changeFilter = (key: KeyType, val: string) => {
    const f = filters.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filters.push({ key, val });
    }
    setFilters([...filters]);
  };
  const filterFunc = (instance: { [key: string]: any }) => {
    return filters.every(
      (f) => !f.val || get(instance, f.key, "").toString().includes(f.val),
    );
  };

  return {
    changeFilter,
    filterFunc,
  };
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
  const open = () => setKey(`ON${Math.random()}`);
  const close = () => setKey(`OFF${Math.random()}`);

  return (
    <React.Fragment>
      {!mini && (
        <div style={{ display: "flex", alignItems: "center" }}>
          <SearchInput
            label="Pid"
            onChange={(value: string) => changeFilter("pid", value)}
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
              "Ops",
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
                slsUrl,
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
                          {key}:{val}
                        </div>
                      ))}
                    </div>
                  </TableCell>
                  <TableCell align="center">
                    <div style={{ maxHeight: 55, overflow: "auto" }}>
                      {Object.entries(memoryInfo || {}).map(([key, val]) => (
                        <div style={{ margin: 4 }}>
                          {key}:{numeral(val).format("0.00b")}
                        </div>
                      ))}
                    </div>
                  </TableCell>
                  <TableCell align="center" style={{ lineBreak: "anywhere" }}>
                    {cmdline && longTextCut(cmdline.filter((e) => e).join(" "))}
                  </TableCell>
                  <TableCell align="center">
                    {moment(createTime * 1000).format("YYYY/MM/DD HH:mm:ss")}
                  </TableCell>
                  <TableCell align="center">
                    {language === "JAVA" && (
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
                    )}
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
