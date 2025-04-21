import DownloadIcon from "@mui/icons-material/Download";
import RefreshIcon from "@mui/icons-material/Refresh";
import {
  Box,
  IconButton,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
} from "@mui/material";
import FormControlLabel from "@mui/material/FormControlLabel";
import Switch from "@mui/material/Switch";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import DebugPanel from "../../components/DebugPanel";
import InsightPanel from "../../components/InsightPanel";
import ElementsPanel from "../../components/ray-visualization/ElementsPanel";
import {
  FlameVisualization,
  FlameVisualizationHandle,
} from "../../components/ray-visualization/FlameVisualization";
import { colorScheme } from "../../components/ray-visualization/graphData";
import InfoCard from "../../components/ray-visualization/InfoCard";
import PhysicalVisualization, {
  PhysicalVisualizationHandle,
} from "../../components/ray-visualization/PhysicalVisualization";
import RayVisualization, {
  RayVisualizationHandle,
} from "../../components/ray-visualization/RayVisualization";
import { FlameGraphData, getFlameGraphData } from "../../service/flame-graph";
import {
  getPhysicalViewData,
  PhysicalViewData,
} from "../../service/physical-view";
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
  callFlows: {
    source: string;
    target: string;
    count: number;
    startTime: number;
  }[];
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
  const [stackGraphData, setStackGraphData] = useState<GraphData | null>(null);
  const [physicalViewData, setPhysicalViewData] =
    useState<PhysicalViewData | null>(null);
  const [flameData, setFlameData] = useState<FlameGraphData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [currentJobId, setCurrentJobId] = useState<string | undefined>(jobId);
  const [searchTerm, setSearchTerm] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [currentViewType, setCurrentViewType] =
    useState<"logical" | "call_stack" | "physical" | "flame" | "analysis">(
      "logical",
    );
  const visualizationRef = useRef<RayVisualizationHandle>(null);
  const physicalVisualizationRef = useRef<PhysicalVisualizationHandle>(null);
  const flameVisualizationRef = useRef<FlameVisualizationHandle>(null);
  const autoRefreshIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const [updating, setUpdating] = useState(false);

  // State management similar to App.tsx
  const [infoCardData, setInfoCardData] = useState<ElementData>({
    id: "default",
    type: "function",
    name: "Component Details",
    language: "unknown",
  });

  const [selectedElementId, setSelectedElementId] =
    useState<string | null>(null);

  const fetchGraphData = useCallback(
    async (id?: string, stackMode?: boolean) => {
      if (!id) {
        return;
      }

      try {
        const path = jobId
          ? `call_graph?job_id=${jobId}${stackMode ? "&stack_mode=1" : ""}`
          : "call_graph";
        const result = await get<GraphDataRsp>(path);

        if (result.data) {
          if (stackMode) {
            setStackGraphData(result.data.data.graphData);
          } else {
            setGraphData(result.data.data.graphData);
          }
          setError(null);
        }
      } catch (err) {
        setError(
          err instanceof Error ? err.message : "Failed to fetch graph data",
        );
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
      (async () => {
        await fetchGraphData(currentJobId, false);
        await fetchGraphData(currentJobId, true);
        const data = await getPhysicalViewData(currentJobId);
        setPhysicalViewData(data);
        const flameData = await getFlameGraphData(currentJobId);
        setFlameData(flameData);
      })();
    }
  }, [currentJobId, fetchGraphData]);

  // eslint-disable-next-line
  const fetchDatas = async () => {
    if (currentViewType === "call_stack") {
      await fetchGraphData(currentJobId, true);
    }
    if (currentViewType === "logical") {
      await fetchGraphData(currentJobId, false);
    }
    if (currentViewType === "physical") {
      await fetchGraphData(currentJobId, false);
      const data = await getPhysicalViewData(currentJobId);
      setPhysicalViewData(data);
    }
    if (currentViewType === "flame" || currentViewType === "analysis") {
      await fetchGraphData(currentJobId, false);
      const data = await getPhysicalViewData(currentJobId);
      setPhysicalViewData(data);
      const flameData = await getFlameGraphData(currentJobId);
      setFlameData(flameData);
    }
  };
  // Auto-refresh effect for call stack view
  useEffect(() => {
    if (autoRefresh) {
      const intervalId = setInterval(async () => {
        await fetchDatas();
      }, 2000);

      autoRefreshIntervalRef.current = intervalId;

      return () => {
        if (autoRefreshIntervalRef.current) {
          clearInterval(autoRefreshIntervalRef.current);
          autoRefreshIntervalRef.current = null;
        }
      };
    } else if (autoRefreshIntervalRef.current) {
      clearInterval(autoRefreshIntervalRef.current);
      autoRefreshIntervalRef.current = null;
    }
    // eslint-disable-next-line
  }, [autoRefresh, currentJobId, fetchGraphData, currentViewType]);

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

  const handleUpdate = useCallback(async () => {
    setUpdating(true);
    await fetchDatas();
    setUpdating(false);
  }, [fetchDatas]);

  const handleSearchChange = useCallback((term: string) => {
    setSearchTerm(term);
  }, []);

  const handleViewTypeChange = useCallback(
    (
      viewType: "logical" | "call_stack" | "physical" | "flame" | "analysis",
    ) => {
      setCurrentViewType(viewType);
    },
    [],
  );

  const handleAutoRefreshChange = useCallback((enabled: boolean) => {
    setAutoRefresh(enabled);
  }, []);

  // Function to handle SVG export based on current view type
  const handleExportSvg = () => {
    switch (currentViewType) {
      case "logical":
      case "call_stack":
        visualizationRef.current?.exportSvg();
        break;
      case "physical":
        physicalVisualizationRef.current?.exportSvg();
        break;
      case "flame":
        flameVisualizationRef.current?.exportSvg();
        break;
      default:
        console.warn("Export not supported for this view type");
    }
  };

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
      <DebugPanel jobId={currentJobId} selectedElement={infoCardData} />
      <Box
        sx={{
          flex: 1,
          display: "flex",
          flexDirection: "column",
          position: "relative",
        }}
      >
        <div className="header">
          <div className="title-container">
            {handleUpdate && (
              <Tooltip title="Update graph">
                <IconButton
                  onClick={handleUpdate}
                  size="small"
                  disabled={updating}
                  sx={{
                    backgroundColor: "white",
                    boxShadow: 1,
                    "&:hover": {
                      backgroundColor: "grey.100",
                    },
                    mt: "4px",
                  }}
                >
                  <RefreshIcon
                    sx={{
                      animation: updating ? "spin 1s linear infinite" : "none",
                      "@keyframes spin": {
                        "0%": {
                          transform: "rotate(0deg)",
                        },
                        "100%": {
                          transform: "rotate(360deg)",
                        },
                      },
                    }}
                  />
                </IconButton>
              </Tooltip>
            )}
            <h1 className="title" style={{ margin: 0 }}>
              Ray Flow Insight
            </h1>
            <React.Fragment>
              <ToggleButtonGroup
                value={currentViewType}
                exclusive
                onChange={(event, value) => {
                  if (value !== null) {
                    handleViewTypeChange(value);
                  }
                }}
                aria-label="view type"
                size="small"
                sx={{ ml: 2 }}
              >
                <ToggleButton value="logical" aria-label="logical view">
                  Logical
                </ToggleButton>
                <ToggleButton value="physical" aria-label="physical view">
                  Physical
                </ToggleButton>
                <ToggleButton value="call_stack" aria-label="call stack view">
                  Call Stack
                </ToggleButton>
                <ToggleButton value="flame" aria-label="flame graph view">
                  Flame Graph
                </ToggleButton>
                <ToggleButton value="analysis" aria-label="analysis view">
                  Analysis
                </ToggleButton>
              </ToggleButtonGroup>

              <FormControlLabel
                control={
                  <Switch
                    checked={autoRefresh}
                    onChange={(e) => handleAutoRefreshChange(e.target.checked)}
                    name="autoRefresh"
                    color="primary"
                  />
                }
                label="Auto Refresh"
                sx={{ ml: 2 }}
              />

              {(currentViewType === "logical" ||
                currentViewType === "call_stack" ||
                currentViewType === "physical" ||
                currentViewType === "flame") && (
                <Tooltip title="Export as SVG">
                  <IconButton
                    onClick={handleExportSvg}
                    size="small"
                    sx={{
                      ml: 2,
                      backgroundColor: "white",
                      boxShadow: 1,
                      "&:hover": {
                        backgroundColor: "grey.100",
                      },
                    }}
                  >
                    <DownloadIcon />
                  </IconButton>
                </Tooltip>
              )}
            </React.Fragment>
          </div>
          <div className="legends">
            <div className="legend-item">
              <span
                className="legend-color"
                style={{ backgroundColor: colorScheme.actorPython }}
              ></span>
              <span>Python Actor</span>
            </div>
            <div className="legend-item">
              <span
                className="legend-color"
                style={{ backgroundColor: colorScheme.method }}
              ></span>
              <span>Method</span>
            </div>
            <div className="legend-item">
              <span
                className="legend-color"
                style={{ backgroundColor: colorScheme.functionPython }}
              ></span>
              <span>Python Function</span>
            </div>
            {searchTerm && searchTerm.trim() !== "" && (
              <div className="legend-item">
                <span
                  className="legend-color"
                  style={{
                    backgroundColor: "white",
                    border: "4px solid #4caf50",
                    borderRadius: "2px",
                    boxSizing: "border-box",
                  }}
                ></span>
                <span>Search Match</span>
              </div>
            )}
          </div>
        </div>

        {graphData && currentViewType === "logical" && (
          <RayVisualization
            ref={visualizationRef}
            // eslint-disable-next-line
            graphData={graphData!}
            physicalViewData={physicalViewData}
            flameData={flameData}
            viewType={currentViewType}
            onElementClick={handleElementClick}
            showInfoCard={true}
            selectedElementId={selectedElementId}
            jobId={currentJobId}
            searchTerm={searchTerm}
            autoRefresh={autoRefresh}
            setViewType={setCurrentViewType}
          />
        )}
        {currentViewType === "call_stack" && (
          <RayVisualization
            ref={visualizationRef}
            // eslint-disable-next-line
            graphData={stackGraphData!}
            physicalViewData={physicalViewData}
            flameData={flameData}
            viewType={currentViewType}
            onElementClick={handleElementClick}
            showInfoCard={true}
            selectedElementId={selectedElementId}
            jobId={currentJobId}
            searchTerm={searchTerm}
            autoRefresh={autoRefresh}
            setViewType={setCurrentViewType}
          />
        )}
        {graphData && currentViewType === "physical" && (
          <PhysicalVisualization
            ref={physicalVisualizationRef}
            // eslint-disable-next-line
            physicalViewData={physicalViewData!}
            onElementClick={handleElementClick}
            selectedElementId={selectedElementId}
            jobId={currentJobId}
            onUpdate={handleUpdate}
            updating={false}
            searchTerm={searchTerm}
          />
        )}
        {flameData && currentViewType === "flame" && (
          <div
            style={{
              position: "relative",
              zIndex: 1,
              width: "100%",
              height: "600px",
            }}
          >
            {flameData ? (
              <FlameVisualization
                ref={flameVisualizationRef}
                flameData={flameData}
                onElementClick={handleElementClick}
                selectedElementId={selectedElementId}
                jobId={currentJobId}
                onUpdate={handleUpdate}
                updating={false}
                searchTerm={searchTerm}
                // eslint-disable-next-line
                graphData={graphData!}
                physicalViewData={physicalViewData || undefined}
              />
            ) : (
              <div className="loading-container">
                <p>No flame graph data available</p>
              </div>
            )}
          </div>
        )}
        {currentViewType === "analysis" && (
          <Box sx={{ position: "relative", width: "100%", height: "100%" }}>
            <InsightPanel
              jobId={currentJobId}
              graphData={graphData}
              physicalViewData={physicalViewData}
              flameData={flameData}
            />
          </Box>
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
          currentView={currentViewType}
          onNavigateToLogicalView={(nodeId) => {
            visualizationRef.current?.navigateToView("logical");
            setSelectedElementId(nodeId);
          }}
        />
      </Box>
    </Box>
  );
};

export default ActorGraph;
