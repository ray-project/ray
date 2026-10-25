import React from "react";
import DataOverviewTable from "../../components/DataOverviewTable";
import { DatasetMetrics } from "../../type/data";

const DataOverview = ({ datasets = [] }: { datasets: DatasetMetrics[] }) => {
  return (
    <div>
      <DataOverviewTable datasets={datasets} />
    </div>
  );
};

export default DataOverview;
