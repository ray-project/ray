import {
  Box,
  Pagination,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import _ from "lodash";
import React, { ReactElement } from "react";
import { sliceToPage } from "../../common/util";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip, StatusChipProps } from "../../components/StatusChip";
import { HelpInfo } from "../../components/Tooltip";
import {
  ServeApplication,
  ServeApplicationsRsp,
  ServeDeployment,
  ServeProxy,
} from "../../type/serve";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { LinkWithArrow } from "../overview/cards/OverviewCard";
import { convertActorStateForServeController } from "./ServeSystemActorDetailPage";
import { ServeControllerRow, ServeProxyRow } from "./ServeSystemDetailRows";

export type ServeDetails = Pick<
  ServeApplicationsRsp,
  "http_options" | "grpc_options" | "proxy_location" | "controller_info"
>;

type ServeSystemDetailsProps = {
  serveDetails: ServeDetails;
  proxies: ServeProxy[];
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
  proxies,
  page,
  setPage,
}: ServeSystemDetailsProps) => {
  const {
    items: list,
    constrainedPage,
    maxPage,
  } = sliceToPage(proxies, page.pageNo, page.pageSize);

  return (
    <div>
      <Typography variant="h3" sx={{ marginBottom: 2 }}>
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
            count={maxPage}
            page={constrainedPage}
            onChange={(e, pageNo) => setPage("pageNo", pageNo)}
          />
        </div>
        <Table>
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
                      <HelpInfo sx={{ marginLeft: 1 }}>{helpInfo}</HelpInfo>
                    )}
                  </Box>
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            <ServeControllerRow controller={serveDetails.controller_info} />
            {list.map((proxy) => (
              <ServeProxyRow key={proxy.actor_id} proxy={proxy} />
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
};

type ServeSystemPreviewProps = {
  serveDetails: ServeDetails;
  proxies: ServeProxy[];
  allDeployments: ServeDeployment[];
  allApplications: ServeApplication[];
};

export const ServeSystemPreview = ({
  serveDetails,
  proxies,
  allDeployments,
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
                elements={proxies}
                statusKey="status"
                type="serveProxy"
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
    <Box display="inline-flex" gap={1} flexWrap="wrap">
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
