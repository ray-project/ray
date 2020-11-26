import { TableRow } from "@material-ui/core";
import React from "react";
import { StyledTableCell } from "../../../common/TableCell";
import { WorkerFeatureData, WorkerFeatureRenderFn } from "./features/types";

type NodeWorkerRowProps = {
  features: WorkerFeatureRenderFn[];
  data: WorkerFeatureData;
};

export const NodeWorkerRow: React.FC<NodeWorkerRowProps> = ({
  features,
  data,
}) => {
  const { node, worker } = data;
  return (
    <TableRow hover>
      <StyledTableCell />
      {features.map((WorkerFeature, index) => (
        <StyledTableCell key={index}>
          <WorkerFeature node={node} worker={worker} />
        </StyledTableCell>
      ))}
    </TableRow>
  );
};
