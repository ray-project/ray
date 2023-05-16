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
} from "@material-ui/core";
import { Pagination } from "@material-ui/lab";
import React, { ReactElement } from "react";
import { RiErrorWarningFill } from "react-icons/ri";
import { CollapsibleSection } from "../../common/CollapsibleSection";
import { MetadataSection } from "../../components/MetadataSection";
import { HelpInfo } from "../../components/Tooltip";
import { ServeApplicationsRsp, ServeHttpProxy } from "../../type/serve";
import { ServeControllerRow, ServeHttpProxyRow } from "./ServeSystemDetailRows";

const useStyles = makeStyles((theme) =>
  createStyles({
    table: {},
    helpInfo: {
      marginLeft: theme.spacing(1),
    },
    errorIcon: {
      color: theme.palette.error.main,
      width: 20,
      height: 20,
    },
  }),
);

export type ServeDetails = Pick<
  ServeApplicationsRsp,
  "http_options" | "proxy_location" | "controller_info"
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

  const isUnhealthy = httpProxies.some(({ status }) => status === "UNHEALTHY");

  return (
    <CollapsibleSection
      title="System"
      startExpanded
      icon={
        isUnhealthy ? (
          <RiErrorWarningFill className={classes.errorIcon} />
        ) : undefined
      }
    >
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
              label: "Port",
              content: {
                copyableValue: `${serveDetails.http_options.port}`,
                value: `${serveDetails.http_options.port}`,
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
    </CollapsibleSection>
  );
};
