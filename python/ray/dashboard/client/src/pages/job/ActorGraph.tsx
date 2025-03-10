import { Box } from "@mui/material";
import React, { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import ElementsPanel from "../../components/ray-visualization/ElementsPanel";
import InfoCard from "../../components/ray-visualization/InfoCard";
import RayVisualization from "../../components/ray-visualization/RayVisualization";

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
  dataFlows: { source: string; target: string; speed: string }[];
};

const ActorGraph = () => {
  const { jobId } = useParams<RouteParams>();
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [updateKey, setUpdateKey] = useState(0);
  const [updating, setUpdating] = useState(false);
  const [currentJobId, setCurrentJobId] = useState<string | undefined>(jobId);

  // State management similar to App.tsx
  const [infoCardData, setInfoCardData] = useState<ElementData>({
    id: "default",
    type: "function",
    name: "Job Actor Details",
    language: "unknown",
  });

  const [selectedElementId, setSelectedElementId] =
    useState<string | null>(null);

  const fetchGraphData = async (id?: string) => {
    if (!id) {
      return;
    }

    try {
      setUpdating(true);
      const response = await fetch(`/call_graph?job_id=${id}`);
      const data = await response.json();

      if (data.result) {
        setGraphData(data.data.graphData);
        setError(null);
        setUpdateKey((prev) => prev + 1);
      } else {
        setError(data.msg || "Failed to fetch graph data");
      }
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to fetch graph data",
      );
    } finally {
      setUpdating(false);
    }
  };

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
  }, [currentJobId]);

  const handleElementClick = useCallback((data: ElementData) => {
    console.log("Element clicked:", data);
    setInfoCardData({ ...data });

    if (data && data.id) {
      setSelectedElementId(data.id);
    }
  }, []);

  const handleUpdate = () => {
    fetchGraphData(currentJobId);
  };

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
