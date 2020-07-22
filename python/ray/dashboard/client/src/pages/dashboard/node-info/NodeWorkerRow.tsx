import { TableRow } from "@material-ui/core";
import React from "react";
import { StyledTableCell } from "../../../common/TableCell";
import { WorkerFeature, WorkerFeatureData } from "./features/types";

type NodeWorkerRowProps = {
  key: string | number;
  features: WorkerFeature[];
  data: WorkerFeatureData;
};

export const NodeWorkerRow: React.FC<NodeWorkerRowProps> = ({
  features,
  data,
  key,
}) => {
  const { node, worker } = data;
  return (
    <TableRow hover key={key}>
      <StyledTableCell />
      {features.map((WorkerFeature, index) => (
        <StyledTableCell key={index}>
          <WorkerFeature node={node} worker={worker} />
        </StyledTableCell>
      ))}
    </TableRow>
  );
};
