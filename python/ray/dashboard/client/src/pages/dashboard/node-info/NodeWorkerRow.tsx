import { TableRow, Theme } from "@material-ui/core";
import React from "react";
import { StyledTableCell } from "../../../common/TableCell";
import { WorkerFeatureComponent, WorkerFeatureData } from "./features/types";

type NodeWorkerRowProps = {
  key: string | number;
  features: WorkerFeatureComponent[];
  data: WorkerFeatureData;
};

export const NodeWorkerRow: React.FC<NodeWorkerRowProps> = ({
  features,
  data,
  key,
}) => {
  const { node, worker, rayletWorker } = data;
  return (
    <TableRow hover key={key}>
      <StyledTableCell />
      {features.map((WorkerFeature, index) => (
        <StyledTableCell key={index}>
          <WorkerFeature
            node={node}
            worker={worker}
            rayletWorker={rayletWorker}
          />
        </StyledTableCell>
      ))}
    </TableRow>
  );
};
