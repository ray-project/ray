import { Box } from "@mui/material";
import React, { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import ElementsPanel from "../../components/ray-visualization/ElementsPanel";
import InfoCard from "../../components/ray-visualization/InfoCard";
import RayVisualization from "../../components/ray-visualization/RayVisualization";
import { get } from "../../service/requestHandlers";

type BaseNode = {
  id: string;
  name: string;
  language: string;
};

type Actor = BaseNode & {
  type: "actor";
  devices: string[];
};

type Method = BaseNode & {
  type: "method";
  actorId: string;
  actorName?: string;
};

type FunctionNode = BaseNode & {
  type: "function";
  actorId?: string;
};

type ElementData = Actor | Method | FunctionNode;

type RouteParams = Record<string, string | undefined>;

// Add after existing type definitions
type GraphData = {
  actors: Actor[];
  methods: Method[];
  functions: FunctionNode[];
  callFlows: { source: string; target: string; count: number }[];
  dataFlows: {
    source: string;
    target: string;
    speed: string;
    timestamp: number;
  }[];
};

type GraphDataRsp = {
  result: boolean;
  msg: string;
  data: {
    graphData: GraphData;
  };
};

const ActorGraph = () => {
  const { jobId } = useParams<RouteParams>();
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [updateKey, setUpdateKey] = useState(0);
  const [updating, setUpdating] = useState(false);
  const [currentJobId, setCurrentJobId] = useState<string | undefined>(jobId);
  const [searchTerm, setSearchTerm] = useState("");

  // State management similar to App.tsx
  const [infoCardData, setInfoCardData] = useState<ElementData>({
    id: "default",
    type: "function",
    name: "Job Actor Details",
    language: "unknown",
  });

  const [selectedElementId, setSelectedElementId] =
    useState<string | null>(null);

  const fetchGraphData = useCallback(
    async (id?: string) => {
      if (!id) {
        return;
      }

      try {
        setUpdating(true);
        const path = jobId ? `call_graph?job_id=${jobId}` : "call_graph";
        const result = await get<GraphDataRsp>(path);

        if (result.data) {
          setGraphData(result.data.data.graphData);
          setError(null);
          setUpdateKey((prev) => prev + 1);
        }
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to fetch graph data",
        );
      } finally {
        setUpdating(false);
      }
    },
    [jobId],
  );

  // Update currentJobId when route jobId changes
  useEffect(() => {
    if (jobId) {
      setCurrentJobId(jobId);
    }
  }, [jobId]);

  // Initial data fetch
  useEffect(() => {
    if (currentJobId) {
      setLoading(true);
      fetchGraphData(currentJobId).finally(() => setLoading(false));
    }
  }, [currentJobId, fetchGraphData]);

  const handleElementClick = useCallback(
    (data: ElementData, skip_zoom = false) => {
      console.log("Element clicked:", data);
      setInfoCardData({ ...data });
      if (skip_zoom) {
        return;
      }

      if (data && data.id) {
        setSelectedElementId(data.id);
      }
    },
    [],
  );

  const handleUpdate = useCallback(() => {
    fetchGraphData(currentJobId);
  }, [fetchGraphData, currentJobId]);

  const handleSearchChange = useCallback((term: string) => {
    setSearchTerm(term);
  }, []);

  if (loading) {
    return <Box>Loading...</Box>;
  }

  if (error) {
    return <Box color="error.main">Error: {error}</Box>;
  }

  return (
    <Box
      sx={{
        display: "flex",
        height: "calc(100vh - 64px)",
        width: "100%",
        position: "relative",
      }}
    >
      <ElementsPanel
        onElementSelect={handleElementClick}
        selectedElementId={selectedElementId || ""}
        onSearchChange={handleSearchChange}
        graphData={
          graphData || {
            actors: [],
            methods: [],
            functions: [],
            callFlows: [],
            dataFlows: [],
          }
        }
      />

      <Box
        sx={{
          flex: 1,
          display: "flex",
          flexDirection: "column",
          position: "relative",
        }}
      >
        {graphData && (
          <RayVisualization
            graphData={graphData}
            onElementClick={handleElementClick}
            showInfoCard={true}
            selectedElementId={selectedElementId}
            jobId={currentJobId}
            updateKey={updateKey}
            onUpdate={handleUpdate}
            updating={updating}
            searchTerm={searchTerm}
          />
        )}

        <InfoCard
          data={infoCardData}
          visible={true}
          graphData={
            graphData || {
              actors: [],
              methods: [],
              functions: [],
              callFlows: [],
              dataFlows: [],
            }
          }
        />
      </Box>
    </Box>
  );
};

export default ActorGraph;
