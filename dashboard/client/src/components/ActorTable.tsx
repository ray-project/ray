import {
  Box,
  InputAdornment,
  Link,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  TextFieldProps,
  Tooltip,
  Typography,
} from "@material-ui/core";
import { orange } from "@material-ui/core/colors";
import { SearchOutlined } from "@material-ui/icons";
import Autocomplete from "@material-ui/lab/Autocomplete";
import Pagination from "@material-ui/lab/Pagination";
import _ from "lodash";
import React, { useMemo, useState } from "react";
import { Link as RouterLink } from "react-router-dom";
import { DurationText, getDurationVal } from "../common/DurationText";
import { ActorLink, generateNodeLink } from "../common/links";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
  MemoryProfilingButton,
} from "../common/ProfilingLink";
import rowStyles from "../common/RowStyles";
import { getSumGpuUtilization, WorkerGpuRow } from "../pages/node/GPUColumn";
import { getSumGRAMUsage, WorkerGRAM } from "../pages/node/GRAMColumn";
import { ActorDetail, ActorEnum } from "../type/actor";
import { Worker } from "../type/worker";
import { memoryConverter } from "../util/converter";
import { useFilter, useSorter } from "../util/hook";
import PercentageBar from "./PercentageBar";
import { SearchSelect } from "./SearchComponent";
import StateCounter from "./StatesCounter";
import { StatusChip } from "./StatusChip";
import { HelpInfo } from "./Tooltip";
import RayletWorkerTable, { ExpandableTableRow } from "./WorkerTable";

export type ActorTableProps = {
  actors: { [actorId: string]: ActorDetail };
  workers?: Worker[];
  jobId?: string | null;
  filterToActorId?: string;
  onFilterChange?: () => void;
  detailPathPrefix?: string;
};

const SEQUENCE = {
  FIRST: 1,
  MIDDLE: 2,
  LAST: 3,
};

type StateOrder = {
  [key in ActorEnum]: number;
};

const stateOrder: StateOrder = {
  [ActorEnum.ALIVE]: SEQUENCE.FIRST,
  [ActorEnum.DEPENDENCIES_UNREADY]: SEQUENCE.MIDDLE,
  [ActorEnum.PENDING_CREATION]: SEQUENCE.MIDDLE,
  [ActorEnum.RESTARTING]: SEQUENCE.MIDDLE,
  [ActorEnum.DEAD]: SEQUENCE.LAST,
};
//type predicate for ActorEnum
const isActorEnum = (state: unknown): state is ActorEnum => {
  return Object.values(ActorEnum).includes(state as ActorEnum);
};

const ActorTable = ({
  actors = {},
  workers = [],
  jobId = null,
  filterToActorId,
  onFilterChange,
  detailPathPrefix = "",
}: ActorTableProps) => {
  const [pageNo, setPageNo] = useState(1);
  const { changeFilter, filterFunc } = useFilter<string>({
    overrideFilters:
      filterToActorId !== undefined
        ? [{ key: "actorId", val: filterToActorId }]
        : undefined,
    onFilterChange,
  });
  const [actorIdFilterValue, setActorIdFilterValue] = useState(filterToActorId);
  const [pageSize, setPageSize] = useState(10);

  const uptimeSorterKey = "fake_uptime_attr";
  const gpuUtilizationSorterKey = "fake_gpu_attr";
  const gramUsageSorterKey = "fake_gram_attr";

  const defaultSorterKey = uptimeSorterKey;
  const { sorterFunc, setOrderDesc, setSortKey, sorterKey, descVal } =
    useSorter(defaultSorterKey);

  //We get a filtered and sorted actor list to render from prop actors
  const sortedActors = useMemo(() => {
    const aggregateUserSortKeys = [
      uptimeSorterKey,
      gpuUtilizationSorterKey,
      gramUsageSorterKey,
    ];
    const actorList = Object.values(actors || {}).filter(filterFunc);
    let actorsSortedUserKey = actorList;
    if (aggregateUserSortKeys.includes(sorterKey)) {
      // Uptime, GPU utilization, and GRAM usage are user specified sort keys but require an aggregate function
      // over the actor attribute, so sorting with sortBy
      actorsSortedUserKey = _.sortBy(actorList, (actor) => {
        const descMultiplier = descVal ? 1 : -1;
        switch (sorterKey) {
          case uptimeSorterKey:
            // Note: Sort by uptime only is re-sorted on re-render (not as uptime value changes)
            const startTime = actor.startTime;
            const endTime = actor.endTime;
            // If actor doesn't have startTime, set uptime to infinity for sort so it appears at the bottom of
            // the table by default
            const uptime =
              startTime && startTime > 0
                ? getDurationVal({ startTime, endTime })
                : Number.POSITIVE_INFINITY;
            // Default sort for uptime should be ascending (default for all others is descending)
            // so multiply by -1
            return uptime * -1 * descMultiplier;
          case gpuUtilizationSorterKey:
            const sumGpuUtilization = getSumGpuUtilization(
              actor.pid,
              actor.gpus,
            );
            return sumGpuUtilization * descMultiplier;
          case gramUsageSorterKey:
            const sumGRAMUsage = getSumGRAMUsage(actor.pid, actor.gpus);
            return sumGRAMUsage * descMultiplier;
          default:
            return 0;
        }
      });
    } else {
      actorsSortedUserKey = actorList.sort(sorterFunc);
    }
    return _.sortBy(actorsSortedUserKey, (actor) => {
      // Always show ALIVE actors at top
      const actorOrder = isActorEnum(actor.state) ? stateOrder[actor.state] : 0;
      return actorOrder;
    });
  }, [actors, sorterKey, sorterFunc, filterFunc, descVal]);

  const list = sortedActors.slice((pageNo - 1) * pageSize, pageNo * pageSize);

  const classes = rowStyles();

  const columns = [
    { label: "" },
    { label: "ID" },
    {
      label: "Class",
      helpInfo: (
        <Typography>
          The class name of the actor. For example, the below actor has a class
          name "Actor".
          <br />
          <br />
          @ray.remote
          <br />
          class Actor:
          <br />
          &emsp;pass
          <br />
        </Typography>
      ),
    },
    {
      label: "Name",
      helpInfo: (
        <Typography>
          The name of the actor given by the "name" argument. For example, this
          actor's name is "unique_name".
          <br />
          <br />
          Actor.options(name="unique_name").remote()
        </Typography>
      ),
    },
    {
      label: "Repr",
      helpInfo: (
        <Typography>
          The repr name of the actor instance defined by __repr__. For example,
          this actor will have repr "Actor1"
          <br />
          <br />
          @ray.remote
          <br />
          class Actor:
          <br />
          &emsp;def __repr__(self):
          <br />
          &emsp;&emsp;return "Actor1"
          <br />
        </Typography>
      ),
    },
    {
      label: "State",
      helpInfo: (
        <Typography>
          The state of the actor. States are documented as a "ActorState" in the
          "gcs.proto" file.
        </Typography>
      ),
    },
    {
      label: "Actions",
      helpInfo: (
        <Typography>
          A list of actions performable on this actor.
          <br />
          - Log: view log messages of this actor. Only available if a node is
          alive.
          <br />
          - Stack Trace: Get a stacktrace of the alive actor.
          <br />- CPU Flame Graph: Get a flamegraph for the next 5 seconds of an
          alive actor.
        </Typography>
      ),
    },
    { label: "Uptime" },
    { label: "Job ID" },
    { label: "PID" },
    { label: "IP" },
    { label: "Node ID" },
    {
      label: "CPU",
      helpInfo: (
        <Typography>
          Hardware CPU usage of this Actor (from Worker Process).
          <br />
          <br />
          Node’s CPU usage is calculated against all CPU cores. Worker Process’s
          CPU usage is calculated against 1 CPU core. As a result, the sum of
          CPU usage from all Worker Processes is not equal to the Node’s CPU
          usage.
        </Typography>
      ),
    },
    {
      label: "Memory",
      helpInfo: (
        <Typography>
          Actor's RAM usage (from Worker Process). <br />
        </Typography>
      ),
    },
    {
      label: "GPU",
      helpInfo: (
        <Typography>
          Usage of each GPU device. If no GPU usage is detected, here are the
          potential root causes:
          <br />
          1. non-GPU Ray image is used on this node. Switch to a GPU Ray image
          and try again. <br />
          2. Non Nvidia GPUs are being used. Non Nvidia GPUs' utilizations are
          not currently supported.
          <br />
          3. pynvml module raises an exception.
        </Typography>
      ),
    },
    {
      label: "GRAM",
      helpInfo: (
        <Typography>
          Actor's GRAM usage (from Worker Process). <br />
        </Typography>
      ),
    },
    {
      label: "Restarted",
      helpInfo: (
        <Typography>
          The total number of the count this actor has been restarted.
        </Typography>
      ),
    },
    {
      label: "Placement group ID",
      helpInfo: (
        <Typography>
          The ID of the placement group this actor is scheduled to.
          <br />
        </Typography>
      ),
    },
    {
      label: "Required resources",
      helpInfo: (
        <Typography>
          The required Ray resources to start an actor.
          <br />
          For example, this actor has GPU:1 required resources.
          <br />
          <br />
          @ray.remote(num_gpus=1)
          <br />
          class Actor:
          <br />
          &emsp;pass
          <br />
        </Typography>
      ),
    },
    {
      label: "Exit detail",
      helpInfo: (
        <Typography>
          The detail of an actor exit. Only available when an actor is dead.
        </Typography>
      ),
    },
  ];

  return (
    <React.Fragment>
      <div style={{ flex: 1, display: "flex", alignItems: "center" }}>
        <Autocomplete
          style={{ margin: 8, width: 120 }}
          options={Array.from(
            new Set(Object.values(actors).map((e) => e.state)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("state", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="State" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          defaultValue={filterToActorId === undefined ? jobId : undefined}
          options={Array.from(
            new Set(Object.values(actors).map((e) => e.jobId)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("jobId", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Job Id" />
          )}
        />
        <Autocomplete
          style={{ margin: 8, width: 150 }}
          options={Array.from(
            new Set(Object.values(actors).map((e) => e.address?.ipAddress)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("address.ipAddress", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="IP" />
          )}
        />
        <Autocomplete
          data-testid="nodeIdFilter"
          style={{ margin: 8, width: 150 }}
          options={Array.from(
            new Set(Object.values(actors).map((e) => e.address?.rayletId)),
          )}
          onInputChange={(_: any, value: string) => {
            changeFilter("address.rayletId", value.trim());
          }}
          renderInput={(params: TextFieldProps) => (
            <TextField {...params} label="Node ID" />
          )}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="PID"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("pid", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="Name"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("name", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="Class"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("actorClass", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="repr"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("reprName", value.trim());
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          value={filterToActorId ?? actorIdFilterValue}
          style={{ margin: 8, width: 120 }}
          label="Actor ID"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              changeFilter("actorId", value.trim());
              setActorIdFilterValue(value);
            },
            endAdornment: (
              <InputAdornment position="end">
                <SearchOutlined />
              </InputAdornment>
            ),
          }}
        />
        <TextField
          style={{ margin: 8, width: 120 }}
          label="Page Size"
          size="small"
          InputProps={{
            onChange: ({ target: { value } }) => {
              setPageSize(Math.min(Number(value), 500) || 10);
            },
            endAdornment: (
              <InputAdornment position="end">Per Page</InputAdornment>
            ),
          }}
        />
        <div data-testid="sortByFilter">
          <span style={{ margin: 8, marginTop: 16 }}>
            <SearchSelect
              label="Sort By"
              options={[
                [uptimeSorterKey, "Uptime"],
                ["processStats.memoryInfo.rss", "Used Memory"],
                ["mem[0]", "Total Memory"],
                ["processStats.cpuPercent", "CPU"],
                // Fake attribute key used when sorting by GPU utilization and
                // GRAM usage because aggregate function required on actor key before sorting.
                [gpuUtilizationSorterKey, "GPU Utilization"],
                [gramUsageSorterKey, "GRAM Usage"],
              ]}
              onChange={(val) => setSortKey(val)}
              showAllOption={false}
              defaultValue={defaultSorterKey}
            />
          </span>
        </div>
        <span style={{ margin: 8, marginTop: 20 }}>
          Reverse:
          <Switch onChange={(_, checked) => setOrderDesc(checked)} />
        </span>
      </div>
      <div style={{ display: "flex", alignItems: "center" }}>
        <div>
          <Pagination
            page={pageNo}
            onChange={(e, num) => setPageNo(num)}
            count={Math.ceil(sortedActors.length / pageSize)}
          />
        </div>
        <div>
          <StateCounter type="actor" list={sortedActors} />
        </div>
      </div>
      <div className={classes.tableContainer}>
        <Table>
          <TableHead>
            <TableRow>
              {columns.map(({ label, helpInfo }) => (
                <TableCell align="center" key={label}>
                  <Box
                    display="flex"
                    justifyContent="center"
                    alignItems="center"
                  >
                    {label}
                    {helpInfo && (
                      <HelpInfo className={classes.helpInfo}>
                        {helpInfo}
                      </HelpInfo>
                    )}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {list.map(
              ({
                actorId,
                actorClass,
                reprName,
                jobId,
                placementGroupId,
                pid,
                address,
                state,
                name,
                numRestarts,
                startTime,
                endTime,
                exitDetail,
                requiredResources,
                gpus,
                processStats,
                mem,
              }) => (
                <ExpandableTableRow
                  length={
                    workers.filter(
                      (e) =>
                        e.pid === pid &&
                        address.ipAddress === e.coreWorkerStats[0].ipAddress,
                    ).length
                  }
                  expandComponent={
                    <RayletWorkerTable
                      actorMap={{}}
                      workers={workers.filter(
                        (e) =>
                          e.pid === pid &&
                          address.ipAddress === e.coreWorkerStats[0].ipAddress,
                      )}
                      mini
                    />
                  }
                  key={actorId}
                >
                  <TableCell align="center">
                    <Tooltip
                      className={classes.idCol}
                      title={actorId}
                      arrow
                      interactive
                    >
                      <div>
                        <ActorLink
                          actorId={actorId}
                          to={
                            detailPathPrefix
                              ? `${detailPathPrefix}/${actorId}`
                              : actorId
                          }
                        />
                      </div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">{actorClass}</TableCell>
                  <TableCell align="center">{name ? name : "-"}</TableCell>
                  <TableCell align="center">
                    {reprName ? reprName : "-"}
                  </TableCell>
                  <TableCell align="center">
                    <StatusChip type="actor" status={state} />
                  </TableCell>
                  <TableCell align="center">
                    <React.Fragment>
                      <ActorLink
                        actorId={actorId}
                        to={
                          detailPathPrefix
                            ? `${detailPathPrefix}/${actorId}`
                            : actorId
                        }
                      >
                        Log
                      </ActorLink>
                      <br />
                      <CpuProfilingLink
                        pid={pid}
                        ip={address?.ipAddress}
                        type=""
                      />
                      <br />
                      <CpuStackTraceLink
                        pid={pid}
                        ip={address?.ipAddress}
                        type=""
                      />
                      <br />
                      <MemoryProfilingButton
                        pid={pid}
                        ip={address?.ipAddress}
                      />
                    </React.Fragment>
                  </TableCell>
                  <TableCell align="center">
                    {startTime && startTime > 0 ? (
                      <DurationText startTime={startTime} endTime={endTime} />
                    ) : (
                      "-"
                    )}
                  </TableCell>
                  <TableCell align="center">{jobId}</TableCell>
                  <TableCell align="center">{pid ? pid : "-"}</TableCell>
                  <TableCell align="center">
                    {address?.ipAddress ? address?.ipAddress : "-"}
                  </TableCell>
                  <TableCell align="center">
                    {address?.rayletId ? (
                      <Tooltip
                        className={classes.idCol}
                        title={address?.rayletId}
                        arrow
                        interactive
                      >
                        <div>
                          <Link
                            component={RouterLink}
                            to={generateNodeLink(address.rayletId)}
                          >
                            {address?.rayletId}
                          </Link>
                        </div>
                      </Tooltip>
                    ) : (
                      "-"
                    )}
                  </TableCell>
                  <TableCell>
                    <PercentageBar
                      num={Number(processStats?.cpuPercent)}
                      total={100}
                    >
                      {processStats?.cpuPercent}
                    </PercentageBar>
                  </TableCell>
                  <TableCell>
                    {mem && (
                      <PercentageBar
                        num={processStats?.memoryInfo.rss}
                        total={mem[0]}
                      >
                        {memoryConverter(processStats?.memoryInfo.rss)}/
                        {memoryConverter(mem[0])}(
                        {(
                          (processStats?.memoryInfo.rss / mem[0]) *
                          100
                        ).toFixed(1)}
                        %)
                      </PercentageBar>
                    )}
                  </TableCell>
                  <TableCell>
                    <WorkerGpuRow workerPID={pid} gpus={gpus} />
                  </TableCell>
                  <TableCell>
                    <WorkerGRAM workerPID={pid} gpus={gpus} />
                  </TableCell>
                  <TableCell
                    align="center"
                    style={{
                      color: Number(numRestarts) > 0 ? orange[500] : "inherit",
                    }}
                  >
                    {numRestarts}
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.idCol}
                      title={placementGroupId ? placementGroupId : "-"}
                      arrow
                      interactive
                    >
                      <div>{placementGroupId ? placementGroupId : "-"}</div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.OverflowCol}
                      title={Object.entries(requiredResources || {}).map(
                        ([key, val]) => (
                          <div style={{ margin: 4 }}>
                            {key}: {val}
                          </div>
                        ),
                      )}
                      arrow
                      interactive
                    >
                      <div>
                        {Object.entries(requiredResources || {})
                          .map(([key, val]) => `${key}: ${val}`)
                          .join(", ")}
                      </div>
                    </Tooltip>
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip
                      className={classes.OverflowCol}
                      title={exitDetail}
                      arrow
                      interactive
                    >
                      <div>{exitDetail}</div>
                    </Tooltip>
                  </TableCell>
                </ExpandableTableRow>
              ),
            )}
          </TableBody>
        </Table>
      </div>
    </React.Fragment>
  );
};

export default ActorTable;
