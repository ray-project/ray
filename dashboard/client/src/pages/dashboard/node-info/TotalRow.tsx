import {
  createStyles,
  makeStyles,
  TableCell,
  TableRow,
  Theme,
} from "@material-ui/core";
import LayersIcon from "@material-ui/icons/Layers";
import React from "react";
import { NodeInfoResponse } from "../../../api";
import { StyledTableCell } from "../../../common/TableCell";
import { ClusterFeatureRenderFn } from "./features/types";

const useTotalRowStyles = makeStyles((theme: Theme) =>
  createStyles({
    cell: {
      borderTopColor: theme.palette.divider,
      borderTopStyle: "solid",
      borderTopWidth: 2,
      padding: theme.spacing(1),
      textAlign: "center",
      "&:last-child": {
        paddingRight: theme.spacing(1),
      },
    },
    totalIcon: {
      color: theme.palette.text.secondary,
      fontSize: "1.5em",
      verticalAlign: "middle",
    },
  }),
);

type TotalRowProps = {
  nodes: NodeInfoResponse["clients"];
  clusterTotalWorkers: number;
  features: (ClusterFeatureRenderFn | undefined)[];
};

const TotalRow: React.FC<TotalRowProps> = ({ nodes, features }) => {
  const classes = useTotalRowStyles();
  return (
    <TableRow hover>
      <TableCell className={classes.cell}>
        <LayersIcon className={classes.totalIcon} />
      </TableCell>
      {features.map((ClusterFeature, index) =>
        ClusterFeature ? (
          <TableCell className={classes.cell} key={index}>
            <ClusterFeature nodes={nodes} />
          </TableCell>
        ) : (
          <StyledTableCell key={index} />
        ),
      )}
    </TableRow>
  );
};

export default TotalRow;
