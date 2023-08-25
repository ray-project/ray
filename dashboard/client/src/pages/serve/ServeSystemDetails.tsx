import {
  Box,
  createStyles,
  makeStyles,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@material-ui/core";
import { Pagination } from "@material-ui/lab";
import _ from "lodash";
import React, { ReactElement } from "react";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip, StatusChipProps } from "../../components/StatusChip";
import { HelpInfo } from "../../components/Tooltip";
import {
  ServeApplication,
  ServeApplicationsRsp,
  ServeHttpProxy,
} from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { LinkWithArrow } from "../overview/cards/OverviewCard";
import { convertActorStateForServeController } from "./ServeSystemActorDetailPage";
import { ServeControllerRow, ServeHttpProxyRow } from "./ServeSystemDetailRows";

const useStyles = makeStyles((theme) =>
  createStyles({
    table: {},
    title: {
      marginBottom: theme.spacing(2),
    },
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
  }),
);

export type ServeDetails = Pick<
  ServeApplicationsRsp,
  "http_options" | "grpc_options" | "proxy_location" | "controller_info"
>;

type ServeSystemDetailsProps = {
  serveDetails: ServeDetails;
  httpProxies: ServeHttpProxy[];
  page: { pageSize: number; pageNo: number };
  setPage: (key: string, value: number) => void;
};

const columns: { label: string; helpInfo?: ReactElement; width?: string }[] = [
  { label: "Name" },
  { label: "Status" },
  { label: "Actions" },
  { label: "Node ID" },
  { label: "Actor ID" },
];

export const ServeSystemDetails = ({
  serveDetails,
  httpProxies,
  page,
  setPage,
}: ServeSystemDetailsProps) => {
  const classes = useStyles();

  return (
    <div>
      <Typography variant="h3" className={classes.title}>
        System
      </Typography>
      {serveDetails.http_options && (
        <MetadataSection
          metadataList={[
            {
              label: "Host",
              content: {
                copyableValue: serveDetails.http_options.host,
                value: serveDetails.http_options.host,
              },
            },
            {
              label: "HTTP Port",
              content: {
                copyableValue: `${serveDetails.http_options.port}`,
                value: `${serveDetails.http_options.port}`,
              },
            },
            {
              label: "gRPC Port",
              content: {
                copyableValue: `${serveDetails.grpc_options.port}`,
                value: `${serveDetails.grpc_options.port}`,
              },
            },
            {
              label: "Proxy location",
              content: {
                value: `${serveDetails.proxy_location}`,
              },
            },
          ]}
        />
      )}
      <TableContainer>
        <div style={{ display: "flex", alignItems: "center" }}>
          <Pagination
            count={Math.ceil(httpProxies.length / page.pageSize)}
            page={page.pageNo}
            onChange={(e, pageNo) => setPage("pageNo", pageNo)}
          />
        </div>
        <Table className={classes.table}>
          <TableHead>
            <TableRow>
              {columns.map(({ label, helpInfo, width }) => (
                <TableCell
                  align="center"
                  key={label}
                  style={width ? { width } : undefined}
                >
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
            <ServeControllerRow controller={serveDetails.controller_info} />
            {httpProxies
              .slice(
                (page.pageNo - 1) * page.pageSize,
                page.pageNo * page.pageSize,
              )
              .map((httpProxy) => (
                <ServeHttpProxyRow
                  key={httpProxy.actor_id}
                  httpProxy={httpProxy}
                />
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
};

type ServeSystemPreviewProps = {
  serveDetails: ServeDetails;
  httpProxies: ServeHttpProxy[];
  allApplications: ServeApplication[];
};

export const ServeSystemPreview = ({
  serveDetails,
  httpProxies,
  allApplications,
}: ServeSystemPreviewProps) => {
  const { data: controllerActor } = useFetchActor(
    serveDetails.controller_info.actor_id,
  );

  if (!controllerActor) {
    return <Loading loading />;
  }

  return (
    <div>
      <MetadataSection
        metadataList={[
          {
            label: "Controller status",
            content: (
              <StatusChip
                type="serveController"
                status={convertActorStateForServeController(
                  controllerActor.state,
                )}
              />
            ),
          },
          {
            label: "Proxy status",
            content: (
              <StatusCountChips
                elements={httpProxies}
                statusKey="status"
                type="serveHttpProxy"
              />
            ),
          },
          {
            label: "Application status",
            content: (
              <StatusCountChips
                elements={allApplications}
                statusKey="status"
                type="serveApplication"
              />
            ),
          },
        ]}
        footer={
          <LinkWithArrow
            text="View system status and configuration"
            to="system"
          />
        }
      />
    </div>
  );
};

type StatusCountChipsProps<T> = {
  elements: T[];
  statusKey: keyof T;
  type: StatusChipProps["type"];
};

const StatusCountChips = <T,>({
  elements,
  statusKey,
  type,
}: StatusCountChipsProps<T>) => {
  const statusCounts = _.mapValues(
    _.groupBy(elements, statusKey),
    (group) => group.length,
  );

  return (
    <Box display="inline-flex" gridGap={8} flexWrap="wrap">
      {_.orderBy(
        Object.entries(statusCounts),
        ([, count]) => count,
        "desc",
      ).map(([status, count]) => (
        <StatusChip
          key={status}
          status={status}
          type={type}
          suffix={<React.Fragment>&nbsp;{`x ${count}`}</React.Fragment>}
        />
      ))}
    </Box>
  );
};
