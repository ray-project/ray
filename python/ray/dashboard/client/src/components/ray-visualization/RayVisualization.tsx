import * as d3 from "d3";
import { Selection, ZoomBehavior } from "d3";
import * as dagre from "dagre";
import * as dagreD3 from "dagre-d3";
import React, {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
} from "react";
import { DebugSession, getDebugSessions } from "../../service/debug";
import { FlameGraphData } from "../../service/flame-graph";
import { PhysicalViewData } from "../../service/physical-view";
import { colorScheme } from "./graphData";
import "./RayVisualization.css";

type RayVisualizationProps = {
  graphData: GraphData;
  viewType: "logical" | "physical" | "flame" | "call_stack";
  physicalViewData: PhysicalViewData | null;
  flameData: FlameGraphData | null;
  onElementClick: (data: any, skip_zoom?: boolean) => void;
  showInfoCard: boolean;
  selectedElementId: string | null;
  jobId?: string;
  updating?: boolean;
  searchTerm?: string;
  onAutoRefreshChange?: (enabled: boolean) => void;
  autoRefresh?: boolean;
  setViewType: (
    viewType: "logical" | "physical" | "flame" | "call_stack",
  ) => void;
};

type NodeData = {
  x: number;
  y: number;
  width: number;
  height: number;
  class?: string;
  originalData?: any;
  actorName?: string;
  actorId?: string;
};

type GraphMap = {
  [key: string]: string[];
};

type VisitedMap = {
  [key: string]: boolean;
};

type Method = {
  id: string;
  actorId?: string;
  name: string;
  language: string;
  actorName?: string;
};

type Actor = {
  id: string;
  name: string;
  language: string;
  gpuDevices?: Array<{
    index: number;
    name: string;
    uuid: string;
    memoryUsed: number;
    memoryTotal: number;
    utilization?: number;
  }>;
};

type FunctionNode = {
  id: string;
  name: string;
  language: string;
  actorId?: string;
};

type DagreNodeConfig = {
  x: number;
  y: number;
  width: number;
  height: number;
  class?: string;
  label?: string;
  padding?: number;
  paddingX?: number;
  paddingY?: number;
  rx?: number;
  ry?: number;
  shape?: string;
  clusterLabelPos?: string;
  originalData?: any;
  style?: string;
};

type SubgraphLayout = {
  graph: dagre.graphlib.Graph;
  nodes: string[];
  width: number;
  height: number;
};

type DagreGraph = dagre.graphlib.Graph & {
  graph(): GraphLabel;
  node(id: string): DagreNodeConfig;
  children(id: string): string[];
};

type GraphLabel = {
  width?: number;
  height?: number;
  rankdir?: string;
};

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
    speed?: string;
    timestamp: number;
    argpos?: number;
    duration?: number;
    size?: number;
  }[];
};

// Add export for the handle type
export type RayVisualizationHandle = {
  navigateToView: (
    viewType: "logical" | "physical" | "flame" | "call_stack",
  ) => void;
  exportSvg: () => void;
};

const RayVisualization = forwardRef<
  RayVisualizationHandle,
  RayVisualizationProps
>(
  (
    {
      graphData,
      physicalViewData,
      flameData,
      onElementClick,
      showInfoCard,
      selectedElementId,
      viewType,
      jobId,
      updating = false,
      searchTerm,
      onAutoRefreshChange,
      autoRefresh = false,
      setViewType,
    },
    ref,
  ) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const [activeDebugSessions, setActiveDebugSessions] = useState<
      DebugSession[]
    >([]);

    // Add refs for tracking the position of the main node
    const previousCenterXRef = useRef<number | null>(null);
    const previousCenterYRef = useRef<number | null>(null);
    const previousScaleRef = useRef<number | null>(null);
    // Add view state

    const svgRef = useRef<SVGSVGElement | null>(null);
    const zoomRef =
      useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
    const graphRef =
      useRef<d3.Selection<SVGGElement, unknown, null, any> | null>(null);
    const dagreGraphRef = useRef<DagreGraph | null>(null);

    // Function to export the SVG visualization
    const exportSvg = () => {
      if (!svgRef.current) {
        return;
      }

      // Get the SVG element
      const svgElement = svgRef.current;

      // Create a copy of the SVG to avoid modifying the original
      const svgCopy = svgElement.cloneNode(true) as SVGSVGElement;

      // Set the proper dimensions and styling
      svgCopy.setAttribute("xmlns", "http://www.w3.org/2000/svg");
      svgCopy.setAttribute("width", svgElement.clientWidth.toString());
      svgCopy.setAttribute("height", svgElement.clientHeight.toString());

      // Convert to a string
      const serializer = new XMLSerializer();
      const svgString = serializer.serializeToString(svgCopy);

      // Create a blob from the SVG string
      const blob = new Blob([svgString], { type: "image/svg+xml" });
      const url = URL.createObjectURL(blob);

      // Create a download link and trigger the download
      const a = document.createElement("a");
      a.href = url;
      a.download = `ray-visualization-${viewType}-${new Date()
        .toISOString()
        .slice(0, 10)}.svg`;
      document.body.appendChild(a);
      a.click();

      // Clean up
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    };

    // Expose navigateToView method
    useImperativeHandle(ref, () => ({
      navigateToView: (
        newViewType: "logical" | "physical" | "flame" | "call_stack",
      ) => {
        setViewType(newViewType);
      },
      exportSvg: exportSvg,
    }));

    // Function to focus on a specific node
    const focusOnNode = useCallback(
      (nodeId: string) => {
        if (!svgRef.current || !zoomRef.current) {
          return;
        }

        const svg = d3.select(svgRef.current);
        const inner = svg.select("g");
        const g = dagreGraphRef.current;
        // Try to find node data from graph structure
        const nodeData = g?.node(nodeId) as NodeData | undefined;

        if (nodeData && nodeData.x && nodeData.y) {
          const svgWidth = parseInt(svg.style("width"));
          const svgHeight = parseInt(svg.style("height"));

          // Get node dimensions - use default if not available
          const nodeWidth = nodeData.width || 100;
          const nodeHeight = nodeData.height || 50;

          // Use a fixed scale based on the maximum dimension of the node
          // This ensures we're showing a consistent level of context
          const maxDimension = Math.max(nodeWidth, nodeHeight);

          // Set a fixed scale based on the maximum dimension
          // Increase all scale values to make nodes appear larger
          let nodeScale;
          if (maxDimension > 300) {
            nodeScale = 0.3; // Very large nodes (increased from 0.15)
          } else if (maxDimension > 200) {
            nodeScale = 0.4; // Large nodes (increased from 0.2)
          } else if (maxDimension > 100) {
            nodeScale = 0.5; // Medium nodes (increased from 0.25)
          } else {
            nodeScale = 0.6; // Small nodes (increased from 0.3)
          }

          // Special handling for actor/cluster nodes
          if (nodeData.class === "actor" || nodeData.class === "cluster") {
            // Actors still need to be zoomed out more to show their children
            // But use a less aggressive reduction factor
            nodeScale *= 0.8; // Increased from 0.7
          }

          // Calculate translation to center the node
          const translateX = svgWidth / 2 - nodeData.x * nodeScale;
          const translateY = svgHeight / 2 - nodeData.y * nodeScale;

          // Apply transform with transition
          (svg.transition() as any)
            .duration(750)
            .call(
              zoomRef.current.transform,
              d3.zoomIdentity
                .translate(translateX, translateY)
                .scale(nodeScale),
            );

          // Highlight the node
          svg
            .selectAll(".node, .cluster, .main-node-container")
            .classed("selected-node", false);
          inner.select(`[id="${nodeId}"]`).classed("selected-node", true);
          const timeout = setTimeout(() => {
            updateParnetRef(inner);
          }, 751);
          return () => clearTimeout(timeout);
        }
      },
      [dagreGraphRef],
    );

    // Watch for changes to selectedElementId and focus on the node
    useEffect(() => {
      if (selectedElementId) {
        const g = dagreGraphRef.current;
        const nodeData = g?.node(selectedElementId);

        // Immediate focus if data exists
        if (nodeData) {
          focusOnNode(selectedElementId);

          // Also highlight related edges when selection changes
          const svg = d3.select(svgRef.current);

          // Reset all edges to transparent first - select ALL edges in both main container and subgraphs
          // Use a more comprehensive selector to ensure we get all edges in all subgraphs
          svg.selectAll("path").each(function () {
            const path = d3.select(this);
            const classes = path.attr("class") || "";
            if (classes.includes("edge") || classes.includes("edgePath")) {
              path.style("opacity", 0.2);
            }
          });

          // Also make all edge labels transparent
          svg.selectAll("text").each(function () {
            const text = d3.select(this);
            const classes = text.attr("class") || "";
            if (
              classes.includes("edge-label") ||
              classes.includes("edgeLabel")
            ) {
              text.style("opacity", 0.2);
            }
          });

          // Check if the selected node is an actor
          const isActor = nodeData && nodeData.class === "actor";

          // If it's an actor, find all its methods
          const relatedNodeIds = [selectedElementId];
          if (isActor) {
            // Find all methods that belong to this actor
            graphData.methods.forEach((method) => {
              if (method.actorId === selectedElementId) {
                relatedNodeIds.push(method.id);
              }
            });
          }

          // Highlight edges connected to this node or its methods
          svg.selectAll("path").each(function () {
            const edge = d3.select(this);
            const classes = edge.attr("class") || "";

            // Only process if it's an edge
            if (!classes.includes("edge")) {
              return;
            }

            const source = edge.attr("data-source");
            const target = edge.attr("data-target");

            // Check if this edge connects to the selected node or any of its methods
            let isConnected = false;
            for (const id of relatedNodeIds) {
              if (
                (source && (source === id || source.includes(id))) ||
                (target && (target === id || target.includes(id)))
              ) {
                isConnected = true;
                break;
              }
            }

            if (isConnected) {
              edge.style("opacity", 1);

              // Find and highlight the corresponding label
              // For call flows, we need to find the label that corresponds to this edge
              if (source && target) {
                svg.selectAll("text").each(function () {
                  const label = d3.select(this);
                  const labelClasses = label.attr("class") || "";

                  // Only process if it's an edge label
                  if (!labelClasses.includes("edge-label")) {
                    return;
                  }

                  // Check if this label is positioned for this edge
                  // We can use the position of the label to determine if it belongs to this edge
                  const labelX = parseFloat(label.attr("x") || "0");
                  const labelY = parseFloat(label.attr("y") || "0");

                  // Get the path data to check if the label is on this path
                  const pathElement = edge.node() as SVGPathElement;
                  if (pathElement && pathElement.getTotalLength) {
                    const pathLength = pathElement.getTotalLength();
                    const midPoint = pathElement.getPointAtLength(
                      pathLength / 2,
                    );

                    // If the label is close to the midpoint of the path, it's likely for this edge
                    const dx = midPoint.x - labelX;
                    const dy = midPoint.y - labelY;
                    const distance = Math.sqrt(dx * dx + dy * dy);

                    // Use a reasonable threshold for proximity
                    if (distance < 30) {
                      label.style("opacity", 1);
                    }
                  }
                });
              }
            }
          });

          return;
        }

        // Fallback with cleanup
        const timeout = setTimeout(() => {
          if (dagreGraphRef.current?.node(selectedElementId)) {
            focusOnNode(selectedElementId);
          }
        }, 100);

        return () => clearTimeout(timeout);
      }
      // eslint-disable-next-line
    }, [selectedElementId, focusOnNode]);

    const fetchActiveDebugSessions = useCallback(async () => {
      if (jobId) {
        try {
          const sessions = await getDebugSessions(jobId, null, null, true);
          setActiveDebugSessions(sessions);
        } catch (error) {
          console.error("Error fetching active debug sessions:", error);
        }
      }
      // eslint-disable-next-line
    }, [jobId, graphData]);

    useEffect(() => {
      fetchActiveDebugSessions();
    }, [jobId, graphData, fetchActiveDebugSessions]);

    // Memoize the renderGraph function with useCallback
    const renderGraph = () => {
      if (!svgRef.current) {
        return;
      }

      // Clear previous graph
      d3.select(svgRef.current).selectAll("*").remove();

      // Create an SVG group for rendering the graph
      const svg = d3.select(svgRef.current) as Selection<
        SVGSVGElement,
        unknown,
        null,
        undefined
      >;
      const inner = svg.append("g") as Selection<
        SVGGElement,
        unknown,
        null,
        undefined
      >;
      graphRef.current = inner as unknown as Selection<
        SVGGElement,
        unknown,
        null,
        any
      >;

      // Set zoom support
      const zoom = d3
        .zoom<SVGSVGElement, unknown>()
        .on("zoom", (event: d3.D3ZoomEvent<SVGSVGElement, unknown>) => {
          inner.attr("transform", event.transform.toString());
        })
        .on("end", (event: d3.D3ZoomEvent<SVGSVGElement, unknown>) => {
          // Track position changes from drag operations and scroll events
          if (
            event.sourceEvent &&
            (event.sourceEvent.type === "mouseup" ||
              event.sourceEvent.type === "touchend" ||
              event.sourceEvent.type === "wheel" ||
              event.sourceEvent.type === "mousewheel")
          ) {
            // Get the inner group's transform
            updateParnetRef(inner);
          }
        });
      (svg as any).call(zoom);
      zoomRef.current = zoom as unknown as ZoomBehavior<SVGSVGElement, unknown>;

      renderCircularSubgraphLayout(
        svg,
        inner,
        zoom as ZoomBehavior<SVGSVGElement, unknown>,
        activeDebugSessions,
      );
    };

    // Find connected subgraphs (excluding main node)
    const renderCircularSubgraphLayout = (
      svg: Selection<SVGSVGElement, unknown, null, undefined>,
      inner: Selection<SVGGElement, unknown, null, undefined>,
      zoom: ZoomBehavior<SVGSVGElement, unknown>,
      activeDebugSessions: DebugSession[],
    ) => {
      // Function to check if a node matches the search term
      const nodeMatchesSearch = (node: any): boolean => {
        if (!searchTerm || searchTerm.trim() === "") {
          return false;
        }

        const searchTermLower = searchTerm.toLowerCase();
        const nodeType =
          node.type ||
          (node.actorId ? "method" : node.language ? "function" : "actor");

        // Check node ID
        if (node.id && node.id.toLowerCase().includes(searchTermLower)) {
          return true;
        }

        // Check node name
        if (node.name && node.name.toLowerCase().includes(searchTermLower)) {
          return true;
        }

        // Only check actor name for actors, not for methods
        // This prevents methods from being highlighted when searching for an actor
        if (
          nodeType === "actor" &&
          node.actorName &&
          node.actorName.toLowerCase().includes(searchTermLower)
        ) {
          return true;
        }

        return false;
      };

      // Function to check if a node has an active debug session
      const hasActiveDebugSession = (
        activeDebugSessions: DebugSession[],
        node: any,
      ): boolean => {
        if (!node || !activeDebugSessions.length) {
          return false;
        }

        const nodeType =
          node.type ||
          (node.actorId ? "method" : node.language ? "function" : "actor");

        return activeDebugSessions.some((session) => {
          if (nodeType === "method") {
            return (
              node.actorName + ":" + node.actorId === session.className &&
              node.name === session.funcName
            );
          } else if (nodeType === "function") {
            return node.name === session.funcName;
          }
          return false;
        });
      };

      // Find connected subgraphs (excluding main node)
      const findConnectedSubgraphs = () => {
        // Create graph representation
        const graph: GraphMap = {};

        // Initialize graph with all nodes
        const allNodes = [
          ...graphData.actors.map((actor) => actor.id),
          ...graphData.methods.map((method) => method.id),
          ...graphData.functions.map((func) => func.id),
        ];

        allNodes.forEach((node) => {
          graph[node] = [];
        });

        // Add edges from call flows
        graphData.callFlows.forEach((flow) => {
          if (flow.source !== "_main" && flow.target !== "_main") {
            graph[flow.source].push(flow.target);
            graph[flow.target].push(flow.source);

            // If methods from different actors are connected, connect the actors
            const sourceMethod = graphData.methods.find(
              (method) => method.id === flow.source,
            );
            const targetMethod = graphData.methods.find(
              (method) => method.id === flow.target,
            );

            if (
              sourceMethod &&
              targetMethod &&
              sourceMethod.actorId &&
              targetMethod.actorId &&
              sourceMethod.actorId !== targetMethod.actorId
            ) {
              // Connect the two actors
              graph[sourceMethod.actorId].push(targetMethod.actorId);
              graph[targetMethod.actorId].push(sourceMethod.actorId);
            }
          }
        });

        // Add edges from data flows
        graphData.dataFlows.forEach((flow) => {
          if (flow.source !== "_main" && flow.target !== "_main") {
            graph[flow.source].push(flow.target);
            graph[flow.target].push(flow.source);

            // If methods from different actors are connected, connect the actors
            const sourceMethod = graphData.methods.find(
              (method) => method.id === flow.source,
            );
            const targetMethod = graphData.methods.find(
              (method) => method.id === flow.target,
            );

            if (
              sourceMethod &&
              targetMethod &&
              sourceMethod.actorId &&
              targetMethod.actorId &&
              sourceMethod.actorId !== targetMethod.actorId
            ) {
              // Connect the two actors
              graph[sourceMethod.actorId].push(targetMethod.actorId);
              graph[targetMethod.actorId].push(sourceMethod.actorId);
            }
          }
        });

        // Add parent-child relationships for methods to their actors
        graphData.methods.forEach((method) => {
          if (method.actorId && !graph[method.id].includes(method.actorId)) {
            graph[method.id].push(method.actorId);
            graph[method.actorId].push(method.id);
          }
        });

        // Find connected components using BFS
        const visited: VisitedMap = {};
        const subgraphs: string[][] = [];

        allNodes.forEach((node) => {
          if (node !== "_main" && !visited[node]) {
            const subgraph: string[] = [];
            const queue: string[] = [node];
            visited[node] = true;

            while (queue.length > 0) {
              const current = queue.shift();
              if (current) {
                subgraph.push(current);

                const neighbors = graph[current] || [];
                neighbors.forEach((neighbor) => {
                  if (neighbor !== "_main" && !visited[neighbor]) {
                    visited[neighbor] = true;
                    queue.push(neighbor);
                  }
                });
              }
            }

            subgraphs.push(subgraph);
          }
        });

        return subgraphs;
      };

      // First, identify the connected subgraphs
      const subgraphs = findConnectedSubgraphs();

      // Helper function to extract speed value from flow
      const getSpeedValue = (flow: any) => {
        // If we have a pre-calculated speedValue, use it directly
        if (typeof flow.speedValue === "number") {
          return flow.speedValue;
        }

        if (flow.speed) {
          // If the speed is already in MB/s format (from our merged flows)
          const mbpsMatch = flow.speed.match(/(\d+(\.\d+)?)\s*MB\/s/);
          if (mbpsMatch) {
            return parseFloat(mbpsMatch[1]);
          }

          // For backward compatibility with older format
          const speedMatch = flow.speed.match(/(\d+)/);
          return speedMatch ? parseInt(speedMatch[0]) : 0;
        }

        // If no speed string but we have size and duration, calculate it
        if (flow.size && flow.duration && flow.duration > 0) {
          return flow.size / flow.duration;
        }

        return 0;
      };

      // Helper function to calculate normalized line width
      const calculateLineWidth = (
        speedValue: number,
        minSpeed: number,
        maxSpeed: number,
      ) => {
        // Set reasonable min and max width values
        const minWidth = 1.5;
        const maxWidth = 6;

        // Default to minimum width if speed is zero or invalid
        if (speedValue <= 0) {
          return `${minWidth}px`;
        }

        // Handle case where min and max are the same (only one data point)
        if (maxSpeed <= minSpeed) {
          return `${(minWidth + maxWidth) / 2}px`;
        }

        // Apply logarithmic scaling for better visual representation
        // This gives more distinction to lower speeds while preventing very high speeds from dominating
        const logMinSpeed = Math.log(Math.max(0.1, minSpeed));
        const logMaxSpeed = Math.log(Math.max(logMinSpeed + 0.1, maxSpeed));
        const logSpeed = Math.log(Math.max(0.1, speedValue));

        // Calculate normalized width using log scale
        const normalizedSpeed =
          (logSpeed - logMinSpeed) / (logMaxSpeed - logMinSpeed);

        // Clamp the result between min and max width
        const width = minWidth + normalizedSpeed * (maxWidth - minWidth);
        const clampedWidth = Math.max(minWidth, Math.min(maxWidth, width));

        return `${clampedWidth.toFixed(1)}px`;
      };

      // Calculate global min and max speeds for ALL data flows
      let minSpeed = Infinity;
      let maxSpeed = -Infinity;

      // Check speeds in main graph data flows
      graphData.dataFlows.forEach((flow) => {
        const speedValue = getSpeedValue(flow);
        if (speedValue > 0) {
          minSpeed = Math.min(minSpeed, speedValue);
          maxSpeed = Math.max(maxSpeed, speedValue);
        }
      });

      // If no valid speeds found, set defaults
      if (minSpeed === Infinity || maxSpeed === -Infinity) {
        minSpeed = 0;
        maxSpeed = 100;
      }

      // Find the main node
      const mainNode = graphData.functions.find(
        (func) => func.id === "_main",
      ) || {
        id: "_main",
        name: "_main",
        language: "python",
      };

      // Create a new directed graph for the entire layout
      const g = new dagreD3.graphlib.Graph({
        compound: true,
        multigraph: true,
      }).setGraph({
        rankdir: "TB",
        nodesep: 50,
        ranksep: 50,
        marginx: 20,
        marginy: 20,
      }) as unknown as DagreGraph;
      dagreGraphRef.current = g;
      g.setDefaultEdgeLabel(() => ({}));

      // Step 1: Add the main node in the center
      g.setNode("_main", {
        label: mainNode.name,
        class: "function main-node",
        style: nodeMatchesSearch(mainNode)
          ? `fill: ${
              mainNode.language === "python"
                ? colorScheme.functionPython
                : colorScheme.functionJava
            }; stroke: #4caf50; stroke-width: 5px; stroke-dasharray: 8,4;`
          : `fill: ${
              mainNode.language === "python"
                ? colorScheme.functionPython
                : colorScheme.functionJava
            }; stroke: #333; stroke-width: 3px;`,
        rx: 5,
        ry: 5,
        width: 120,
        height: 120,
        paddingLeft: 20,
        paddingRight: 20,
        paddingTop: 15,
        paddingBottom: 15,
        originalData: { ...mainNode, type: "function" },
      });

      // Step 2: Create individual subgraph layouts
      const subgraphLayouts: SubgraphLayout[] = [];

      subgraphs.forEach((subgraph) => {
        // Create a separate graph for each subgraph
        const subG = new dagreD3.graphlib.Graph({
          compound: true,
          multigraph: true,
        }).setGraph({
          rankdir: "LR", // Always use LR for subgraphs in circular layout
        });
        subG.setDefaultEdgeLabel(() => ({}));

        // Add nodes to the subgraph
        subgraph.forEach((nodeId) => {
          let nodeData;
          let nodeType;

          // Find the node data
          const actorData = graphData.actors.find(
            (actor) => actor.id === nodeId,
          );
          if (actorData) {
            nodeData = actorData;
            nodeType = "actor";
          } else {
            const methodData = graphData.methods.find(
              (method) => method.id === nodeId,
            );
            if (methodData) {
              nodeData = methodData as Method;
              // Find actor this method belongs to
              const actor = graphData.actors.find(
                (a) => a.id === methodData.actorId,
              );
              nodeData.actorName = actor ? actor.name : "Unknown Actor";
              nodeType = "method";
            } else {
              const funcData = graphData.functions.find(
                (func) => func.id === nodeId,
              );
              if (funcData) {
                nodeData = funcData;
                nodeType = "function";
              }
            }
          }

          if (nodeData) {
            // Add node to graph with proper styling
            let label = nodeData.name;
            if (nodeType === "actor" && label.length > 20) {
              label = label.substring(0, 20) + "...";
            }

            let style = "";
            let paddingLeft = 10;
            let paddingRight = 10;
            let paddingTop = 8;
            let paddingBottom = 8;

            // Check if node matches search term and highlight it
            const matchesSearch = nodeMatchesSearch(nodeData);

            if (nodeType === "actor") {
              style = `fill: ${
                nodeData.language === "python"
                  ? colorScheme.actorPython
                  : nodeData.language === "cpp"
                  ? colorScheme.actorCpp
                  : colorScheme.actorJava
              }; stroke: ${matchesSearch ? "#4caf50" : "#999"}; stroke-width: ${
                matchesSearch ? "5px" : "1px"
              }; ${matchesSearch ? "stroke-dasharray: 8,4;" : ""}`;
              paddingLeft = 25;
              paddingRight = 25;
              paddingTop = 30;
              paddingBottom = 15;
            } else if (nodeType === "method") {
              style = `fill: ${colorScheme.method}; stroke: ${
                matchesSearch ? "#4caf50" : "#999"
              }; stroke-width: ${matchesSearch ? "5px" : "1px"}; ${
                matchesSearch ? "stroke-dasharray: 8,4;" : ""
              }`;
            } else if (nodeType === "function") {
              style = `fill: ${
                nodeData.language === "python"
                  ? colorScheme.functionPython
                  : nodeData.language === "cpp"
                  ? colorScheme.functionCpp
                  : colorScheme.functionJava
              }; stroke: ${matchesSearch ? "#4caf50" : "#999"}; stroke-width: ${
                matchesSearch ? "5px" : "1px"
              }; ${matchesSearch ? "stroke-dasharray: 8,4;" : ""}`;
              paddingLeft = 15;
              paddingRight = 15;
              paddingTop = 10;
              paddingBottom = 10;
            }

            // Add node to the subgraph
            subG.setNode(nodeId, {
              label:
                label +
                (hasActiveDebugSession(activeDebugSessions, nodeData)
                  ? " 🐞"
                  : ""),
              class: nodeType,
              style: style,
              rx: 5,
              ry: 5,
              paddingLeft: paddingLeft,
              paddingRight: paddingRight,
              paddingTop: paddingTop,
              paddingBottom: paddingBottom,
              originalData: { ...nodeData, type: nodeType },
            });

            if (nodeType === "actor") {
              // Ensure actor labels appear at the top
              (subG.node(nodeId) as DagreNodeConfig).clusterLabelPos = "top";
            }

            // Set parent-child relationships for methods
            if (nodeType === "method" && (nodeData as Method).actorId) {
              const methodData = nodeData as Method;
              subG.setParent(nodeId, methodData.actorId!); // eslint-disable-line @typescript-eslint/no-non-null-assertion
            }

            // Also add all nodes to the main graph for edge connections
            g.setNode(nodeId, {
              label: label,
              class: nodeType,
              style: style,
              rx: 5,
              ry: 5,
              clusterLabelPos: "top",
              paddingLeft: paddingLeft,
              paddingRight: paddingRight,
              paddingTop: paddingTop,
              paddingBottom: paddingBottom,
              originalData: { ...nodeData, type: nodeType },
            });

            if (nodeType === "actor") {
              // Ensure actor labels appear at the top
              (g.node(nodeId) as DagreNodeConfig).clusterLabelPos = "top";
            }

            if (
              nodeType === "method" &&
              (nodeData as Method).actorId &&
              subgraph.includes((nodeData as Method).actorId!) // eslint-disable-line @typescript-eslint/no-non-null-assertion
            ) {
              // eslint-disable-line @typescript-eslint/no-non-null-assertion
              g.setParent(nodeId, (nodeData as Method).actorId!); // eslint-disable-line @typescript-eslint/no-non-null-assertion
            }
          }
        });

        // Add internal edges (edges between nodes within this subgraph)
        graphData.callFlows.forEach((flow, i) => {
          if (
            subgraph.includes(flow.source) &&
            subgraph.includes(flow.target)
          ) {
            let label = `${flow.count} times`;
            if (viewType === "call_stack") {
              const duration = Date.now() / 1000 - flow.startTime;
              label = `${
                flow.count
              } times [since last call:  ${duration.toFixed(2)}s]`;
            }
            subG.setEdge(
              flow.source,
              flow.target,
              {
                label: label,
                style: "stroke: #000; stroke-width: 2px; fill: none;",
                arrowheadStyle: "fill: #000; stroke: none;",
                curve: d3.curveBasis,
                class: "subgraph-internal-edge",
                source: flow.source,
                target: flow.target,
              },
              `call_${i}`,
            );
          }
        });

        graphData.dataFlows.forEach((flow, i) => {
          if (
            subgraph.includes(flow.source) &&
            subgraph.includes(flow.target)
          ) {
            const speedValue = getSpeedValue(flow);
            const lineWidth = calculateLineWidth(
              speedValue,
              minSpeed,
              maxSpeed,
            );

            subG.setEdge(
              flow.source,
              flow.target,
              {
                style: `stroke: #f5222d; stroke-width: ${lineWidth}; stroke-dasharray: 5, 5; fill: none;`,
                arrowheadStyle: "fill: #f5222d; stroke: none;",
                curve: d3.curveBasis,
                class: "subgraph-internal-edge",
                source: flow.source,
                target: flow.target,
              },
              `data_${i}`,
            );
          }
        });

        // Layout the subgraph
        dagre.layout(subG as any);

        // Store the laid out subgraph
        subgraphLayouts.push({
          nodes: subgraph,
          graph: subG as unknown as dagre.graphlib.Graph<DagreNodeConfig>,
          width: (subG.graph() as dagre.GraphLabel).width || 0,
          height: (subG.graph() as dagre.GraphLabel).height || 0,
        });
      });

      // Step 3: Position subgraphs in a circle with dynamic sizing
      const numSubgraphs = subgraphLayouts.length;
      const subgraphSizes = subgraphLayouts.map((layout) => {
        const g = layout.graph.graph();
        return {
          width: g.width!, // eslint-disable-line @typescript-eslint/no-non-null-assertion
          height: g.height!, // eslint-disable-line @typescript-eslint/no-non-null-assertion
          diagonal: Math.sqrt(g.width! ** 2 + g.height! ** 2), // eslint-disable-line @typescript-eslint/no-non-null-assertion
        };
      });

      // Calculate a better radius based on the number and size of subgraphs
      // Use the maximum subgraph size to ensure enough space between subgraphs
      const maxSubgraphSize = Math.max(...subgraphSizes.map((s) => s.diagonal));
      // Calculate the minimum radius needed to fit all subgraphs without overlap
      // Add a scaling factor based on the number of subgraphs
      const scalingFactor = Math.max(1.5, 1 + numSubgraphs / 10);
      const radius = Math.max(
        400,
        (maxSubgraphSize * numSubgraphs * scalingFactor) / (2 * Math.PI),
      );

      const svgWidth = parseInt(svg.style("width"));
      const svgHeight = parseInt(svg.style("height"));

      // Default center position
      const centerX = svgWidth / 2;
      const centerY = svgHeight / 2;

      // Create a container for our visualization
      const mainContainer = inner.append("g");

      // Create and position main node first to ensure it's on top
      const mainNodeContainer = mainContainer
        .append("g")
        .attr("transform", `translate(${centerX},${centerY})`)
        .attr("class", "_main-node-container")
        .attr("id", "_main")
        .style("cursor", "pointer")
        .on("click", (event) => {
          event.stopPropagation();
          // Update graph node position before focusing
          const g = dagreGraphRef.current;
          if (g) {
            const mainNode = g.node("_main");
            if (mainNode) {
              mainNode.x = centerX;
              mainNode.y = centerY;
            }
          }
          if (mainNode) {
            onElementClick({ ...mainNode, type: "function" });
          }
        })
        .raise();

      // Add a highlight circle as the main node
      mainNodeContainer
        .append("circle")
        .attr("r", 60)
        .attr(
          "fill",
          mainNode.language === "python"
            ? colorScheme.functionPython
            : colorScheme.functionJava,
        )
        .attr("stroke", "#333")
        .attr("stroke-width", 3);

      // Add main node label
      mainNodeContainer
        .append("text")
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("font-size", "14px")
        .attr("font-weight", "bold")
        .attr("fill", "#000")
        .text("main");

      // Add each subgraph to the main visualization
      subgraphLayouts.forEach((subLayout, index) => {
        const angle = (2 * Math.PI * index) / numSubgraphs;
        const x = centerX + radius * Math.cos(angle);
        const y = centerY + radius * Math.sin(angle);

        // Get the dimensions of the subgraph
        const subgraphWidth = subLayout.graph.graph().width!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
        const subgraphHeight = subLayout.graph.graph().height!; // eslint-disable-line @typescript-eslint/no-non-null-assertion

        // Create a container for this subgraph
        const subContainer = mainContainer
          .append("g")
          .attr("transform", `translate(${x},${y})`);

        // Center the subgraph in its container
        const subgraphContainer = subContainer
          .append("g")
          .attr(
            "transform",
            `translate(${-subgraphWidth / 2},${-subgraphHeight / 2})`,
          );

        // Render the subgraph in its container
        const subRenderer = new dagreD3.render();

        // Add style modification for non-matching nodes
        subLayout.nodes.forEach((nodeId) => {
          const node = subLayout.graph.node(nodeId) as DagreNodeConfig;
          if (node && node.originalData) {
            const matches = nodeMatchesSearch(node.originalData);
            const baseStyle = node.style || "";
            const opacity = searchTerm && !matches ? 0.3 : 1;

            node.style = baseStyle.replace(
              /fill: ([^;]+);/,
              `fill: $1; opacity: ${opacity};`,
            );
          }
        });

        subRenderer(subgraphContainer as any, subLayout.graph);

        // Get coordinates of nodes in the subgraph relative to the main graph
        subLayout.nodes.forEach((nodeId) => {
          const node = subLayout.graph.node(nodeId);
          if (node) {
            // Store the global position of this node, properly translating from
            // the subgraph's local coordinates to global coordinates
            const nodeX = node.x || 0;
            const nodeY = node.y || 0;
            const globalX = x + (nodeX - subgraphWidth / 2);
            const globalY = y + (nodeY - subgraphHeight / 2);

            // Update the node in the main graph with this position
            const mainNode = g.node(nodeId);
            if (mainNode) {
              mainNode.x = globalX;
              mainNode.y = globalY;
            }
          }
        });
      });

      // Step 4: Add cross-subgraph edges and edges to/from main

      // Create a unified edge grouping system that considers both call flows and data flows
      const unifiedEdgeGroups = new Map<
        string,
        Array<{
          flow: any;
          index: number;
          type: "call" | "data";
        }>
      >();

      // Group call flows
      graphData.callFlows.forEach((flow, index) => {
        const key = `${flow.source}-${flow.target}`;
        if (!unifiedEdgeGroups.has(key)) {
          unifiedEdgeGroups.set(key, []);
        }
        unifiedEdgeGroups.get(key)?.push({ flow, index, type: "call" });
      });

      // Group data flows in the same map
      graphData.dataFlows.forEach((flow, index) => {
        const key = `${flow.source}-${flow.target}`;
        if (!unifiedEdgeGroups.has(key)) {
          unifiedEdgeGroups.set(key, []);
        }
        unifiedEdgeGroups.get(key)?.push({ flow, index, type: "data" });
      });

      // Process and merge data flows with the same source and target but different argpos
      const mergedDataFlows = new Map<string, any>();

      // First, group data flows by source-target and argpos
      const dataFlowsByArgpos = new Map<string, Map<number, any[]>>();

      graphData.dataFlows.forEach((flow) => {
        const key = `${flow.source}-${flow.target}`;
        const argpos = flow.argpos || 0;

        if (!dataFlowsByArgpos.has(key)) {
          dataFlowsByArgpos.set(key, new Map<number, any[]>());
        }

        const argposMap = dataFlowsByArgpos.get(key)!; // eslint-disable-line
        if (!argposMap.has(argpos)) {
          argposMap.set(argpos, []);
        }

        argposMap.get(argpos)!.push(flow); // eslint-disable-line
      });

      // Then, for each source-target pair, merge flows by argpos
      dataFlowsByArgpos.forEach((argposMap, key) => {
        // For each argpos, take the latest flow based on timestamp
        argposMap.forEach((flows, argpos) => {
          // Sort by timestamp (descending) and take the latest
          flows.sort((a, b) => b.timestamp - a.timestamp);
          const latestFlow = flows[0];

          // If this is the first argpos for this source-target pair, create a new merged flow
          if (!mergedDataFlows.has(key)) {
            mergedDataFlows.set(key, {
              source: latestFlow.source,
              target: latestFlow.target,
              size: latestFlow.size || 0,
              duration: latestFlow.duration || 0,
              timestamp: latestFlow.timestamp,
              argpos: latestFlow.argpos,
            });
          } else {
            // Otherwise, add to the existing merged flow
            const mergedFlow = mergedDataFlows.get(key);
            mergedFlow.size = (mergedFlow.size || 0) + (latestFlow.size || 0);
            mergedFlow.duration = Math.max(
              mergedFlow.duration || 0,
              latestFlow.duration || 0,
            );
            // Keep the latest timestamp
            if (latestFlow.timestamp > mergedFlow.timestamp) {
              mergedFlow.timestamp = latestFlow.timestamp;
            }
          }
        });

        // Calculate speed for the merged flow
        const mergedFlow = mergedDataFlows.get(key);
        if (mergedFlow.size && mergedFlow.duration && mergedFlow.duration > 0) {
          // Calculate speed in MB/s
          const speedMBps = mergedFlow.size / mergedFlow.duration;
          // Format with 2 decimal places for better readability
          mergedFlow.speed = `${speedMBps.toFixed(2)} MB/s`;

          // Also store the raw speed value for easier access in width calculations
          mergedFlow.speedValue = speedMBps;
        } else {
          // Set a default speed if we can't calculate it
          mergedFlow.speed = "0 MB/s";
          mergedFlow.speedValue = 0;
        }
      });

      // Replace the original data flows with merged ones in the unified edge groups
      unifiedEdgeGroups.forEach((edges, key) => {
        // Filter out data flows
        const nonDataEdges = edges.filter((edge) => edge.type !== "data");

        // If we have a merged data flow for this key, add it
        if (mergedDataFlows.has(key)) {
          const mergedFlow = mergedDataFlows.get(key);
          nonDataEdges.push({
            flow: mergedFlow,
            index: 0, // Use index 0 for merged flows
            type: "data",
          });
        }

        // Update the unified edge groups
        unifiedEdgeGroups.set(key, nonDataEdges);
      });

      // Process all edges (both call flows and data flows)
      unifiedEdgeGroups.forEach((edges, key) => {
        // Process each group of edges between the same source and target
        edges.forEach((edge, groupIndex) => {
          const flow = edge.flow;
          const isDataFlow = edge.type === "data";

          // Add only edges that haven't been added to subgraphs
          // or edges that connect to the main node
          const sourceNode = g.node(flow.source);
          const targetNode = g.node(flow.target);

          if (sourceNode && targetNode) {
            // If either node is main or they're in different subgraphs
            if (
              flow.source === "_main" ||
              flow.target === "_main" ||
              !subgraphs.some(
                (subgraph) =>
                  subgraph.includes(flow.source) &&
                  subgraph.includes(flow.target),
              )
            ) {
              // Calculate offset for multiple edges between the same nodes
              // Only apply offset if there are multiple edges
              const edgeOffset =
                edges.length > 1
                  ? (groupIndex - (edges.length - 1) / 2) * 15
                  : 0;

              // For data flows, calculate normalized line width based on speed
              let lineWidth = "2px";
              if (isDataFlow && flow.speed) {
                const speedValue = getSpeedValue(flow);
                lineWidth = calculateLineWidth(speedValue, minSpeed, maxSpeed);
              }

              // Create a path for this edge
              mainContainer
                .append("path")
                .attr("d", () => {
                  let sourceX =
                    flow.source === "_main" ? centerX : sourceNode.x;
                  let sourceY =
                    flow.source === "_main" ? centerY : sourceNode.y;
                  let targetX =
                    flow.target === "_main" ? centerX : targetNode.x;
                  let targetY =
                    flow.target === "_main" ? centerY : targetNode.y;

                  // Calculate control points to create a curved path
                  // For edges to/from main, create a curve that looks more natural
                  let cp1x, cp1y, cp2x, cp2y;

                  if (flow.source === "_main" || flow.target === "_main") {
                    // Calculate a better connection point on the main node perimeter
                    let mainNodeX = centerX;
                    let mainNodeY = centerY;

                    // Main node dimensions - using a circle with radius of 60px
                    const mainNodeRadius = 60;

                    // Calculate the angle between main node and the other node
                    const otherNodeX =
                      flow.source === "_main" ? targetX : sourceX;
                    const otherNodeY =
                      flow.source === "_main" ? targetY : sourceY;

                    // Calculate angle between main node and other node
                    const dx = otherNodeX - centerX;
                    const dy = otherNodeY - centerY;
                    const angle = Math.atan2(dy, dx);

                    // Find point on the main node's perimeter in the direction of the other node
                    if (flow.source === "_main") {
                      mainNodeX = centerX + Math.cos(angle) * mainNodeRadius;
                      mainNodeY = centerY + Math.sin(angle) * mainNodeRadius;
                      // Update source point
                      sourceX = mainNodeX;
                      sourceY = mainNodeY;
                    } else if (flow.target === "_main") {
                      mainNodeX = centerX + Math.cos(angle) * mainNodeRadius;
                      mainNodeY = centerY + Math.sin(angle) * mainNodeRadius;
                      targetX = mainNodeX;
                      targetY = mainNodeY;
                    }

                    // Then calculate the curve
                    const curveStrength = 0.5; // Higher values make more curved paths
                    const midX = sourceX + (targetX - sourceX) * 0.5;
                    const midY = sourceY + (targetY - sourceY) * 0.5;

                    // Create perpendicular offset for curve
                    const perpX = -(targetY - sourceY) * curveStrength;
                    const perpY = (targetX - sourceX) * curveStrength;

                    // Apply offset for multiple edges
                    const perpLength = Math.sqrt(perpX * perpX + perpY * perpY);
                    const normalizedPerpX =
                      perpLength > 0 ? perpX / perpLength : 0;
                    const normalizedPerpY =
                      perpLength > 0 ? perpY / perpLength : 0;

                    cp1x = midX + perpX + normalizedPerpX * edgeOffset;
                    cp1y = midY + perpY + normalizedPerpY * edgeOffset;
                    cp2x = cp1x;
                    cp2y = cp1y;
                  } else {
                    // For edges between subgraphs, make a nice curved path that avoids the center
                    // Calculate the midpoint of the line connecting source and target
                    const midX = (sourceX + targetX) / 2;
                    const midY = (sourceY + targetY) / 2;

                    // Calculate the vector from the center to this midpoint
                    const fromCenterX = midX - centerX;
                    const fromCenterY = midY - centerY;

                    // Calculate the distance from center to midpoint
                    const distFromCenter = Math.sqrt(
                      fromCenterX * fromCenterX + fromCenterY * fromCenterY,
                    );

                    // Calculate a point that's a bit further away from the center
                    const factor = 1.4; // Make the curve pull outward more
                    const curvePointX =
                      centerX +
                      (fromCenterX / distFromCenter) * distFromCenter * factor;
                    const curvePointY =
                      centerY +
                      (fromCenterY / distFromCenter) * distFromCenter * factor;

                    // Apply offset for multiple edges
                    const perpX = -(targetY - sourceY);
                    const perpY = targetX - sourceX;
                    const perpLength = Math.sqrt(perpX * perpX + perpY * perpY);
                    const normalizedPerpX =
                      perpLength > 0 ? perpX / perpLength : 0;
                    const normalizedPerpY =
                      perpLength > 0 ? perpY / perpLength : 0;

                    cp1x = curvePointX + normalizedPerpX * edgeOffset;
                    cp1y = curvePointY + normalizedPerpY * edgeOffset;
                    cp2x = cp1x;
                    cp2y = cp1y;
                  }

                  return `M${sourceX},${sourceY}C${cp1x},${cp1y} ${cp2x},${cp2y} ${targetX},${targetY}`;
                })
                .attr(
                  "style",
                  isDataFlow
                    ? `stroke: #f5222d; stroke-width: ${lineWidth}; stroke-dasharray: 5, 5; fill: none;`
                    : "stroke: #000; stroke-width: 2px; fill: none;",
                )
                .attr(
                  "marker-end",
                  isDataFlow ? "url(#redarrowhead)" : "url(#arrowhead)",
                )
                .attr("class", `edge ${isDataFlow ? "data-flow" : "call-flow"}`)
                .attr("data-source", flow.source)
                .attr("data-target", flow.target);

              // Add a label if needed (only for call flows)
              if (!isDataFlow) {
                let label = `${flow.count} times`;
                if (viewType === "call_stack") {
                  const duration = Date.now() / 1000 - flow.startTime;
                  label = `${
                    flow.count
                  } times [since last call:  ${duration.toFixed(2)}s]`;
                }

                // For call flows, always add the count label
                const callFlowLabel = mainContainer
                  .append("text")
                  .attr("class", "edge-label flow-label")
                  .attr("text-anchor", "middle")
                  .attr("fill", "#000")
                  .attr("font-size", "12px")
                  .attr("font-weight", "bold")
                  .text(label);

                // Position the label after rendering
                positionEdgeLabel(
                  callFlowLabel,
                  flow,
                  sourceNode,
                  targetNode,
                  centerX,
                  centerY,
                  edgeOffset,
                );
              }
            }
          }
        });
      });

      // Helper function to position edge labels consistently
      function positionEdgeLabel( // eslint-disable-line
        label: d3.Selection<SVGTextElement, unknown, null, undefined>,
        flow: any,
        sourceNode: any,
        targetNode: any,
        centerX: number,
        centerY: number,
        edgeOffset: number,
      ) {
        label
          .attr("x", () => {
            const sourceX = flow.source === "_main" ? centerX : sourceNode.x;
            const targetX = flow.target === "_main" ? centerX : targetNode.x;
            const sourceY = flow.source === "_main" ? centerY : sourceNode.y;
            const targetY = flow.target === "_main" ? centerY : targetNode.y;

            if (flow.source === "_main" || flow.target === "_main") {
              // For main node connections, calculate control points the same way as the path
              const otherNodeX = flow.source === "_main" ? targetX : sourceX;
              const otherNodeY = flow.source === "_main" ? targetY : sourceY;

              // Calculate a better connection point on the main node perimeter
              let mainNodeX = centerX;
              let mainNodeY = centerY;

              // Main node dimensions - using a circle with radius of 60px
              const mainNodeRadius = 60;

              // Calculate angle between main node and other node
              const dx = otherNodeX - centerX;
              const dy = otherNodeY - centerY;
              const angle = Math.atan2(dy, dx);

              // Find point on the main node's perimeter in the direction of the other node
              if (flow.source === "_main") {
                mainNodeX = centerX + Math.cos(angle) * mainNodeRadius;
                mainNodeY = centerY + Math.sin(angle) * mainNodeRadius;
              } else if (flow.target === "_main") {
                mainNodeX = centerX + Math.cos(angle) * mainNodeRadius;
                mainNodeY = centerY + Math.sin(angle) * mainNodeRadius;
              }

              // Update source and target points based on which is the main node
              const updatedSourceX =
                flow.source === "_main" ? mainNodeX : sourceX;
              const updatedSourceY =
                flow.source === "_main" ? mainNodeY : sourceY;
              const updatedTargetX =
                flow.target === "_main" ? mainNodeX : targetX;
              const updatedTargetY =
                flow.target === "_main" ? mainNodeY : targetY;

              // Calculate control points for the curve - using the same logic as the edge path
              const curveStrength = 0.5;
              const midX =
                updatedSourceX + (updatedTargetX - updatedSourceX) * 0.5;

              // Create perpendicular offset for curve
              const perpX = -(updatedTargetY - updatedSourceY) * curveStrength;

              // Apply the same offset as the edge
              const perpLength = Math.sqrt(perpX * perpX + perpX * perpX);
              const normalizedPerpX = perpLength > 0 ? perpX / perpLength : 0;
              const cp1x = midX + perpX + normalizedPerpX * edgeOffset;

              // Position label at a point ~60% along the Bezier curve
              // Using the Bezier formula to find a point along the curve
              const t = 0.6; // Parameter between 0 and 1
              const labelX =
                Math.pow(1 - t, 3) * updatedSourceX +
                3 * Math.pow(1 - t, 2) * t * cp1x +
                3 * (1 - t) * Math.pow(t, 2) * cp1x +
                Math.pow(t, 3) * updatedTargetX;

              return labelX;
            } else {
              // For other connections, use midpoint with offset
              const midX = (sourceX + targetX) / 2;

              // Apply offset for multiple edges
              const perpX = -(targetY - sourceY);
              const perpLength = Math.sqrt(perpX * perpX + perpX * perpX);
              const normalizedPerpX = perpLength > 0 ? perpX / perpLength : 0;

              return midX + normalizedPerpX * edgeOffset;
            }
          })
          .attr("y", () => {
            const sourceX = flow.source === "_main" ? centerX : sourceNode.x;
            const targetX = flow.target === "_main" ? centerX : targetNode.x;
            const sourceY = flow.source === "_main" ? centerY : sourceNode.y;
            const targetY = flow.target === "_main" ? centerY : targetNode.y;

            if (flow.source === "_main" || flow.target === "_main") {
              // For main node connections, calculate control points the same way as the path
              const otherNodeX = flow.source === "_main" ? targetX : sourceX;
              const otherNodeY = flow.source === "_main" ? targetY : sourceY;

              // Calculate a better connection point on the main node perimeter
              let mainNodeX = centerX;
              let mainNodeY = centerY;

              // Main node dimensions - using a circle with radius of 60px
              const mainNodeRadius = 60;

              // Calculate angle between main node and other node
              const dx = otherNodeX - centerX;
              const dy = otherNodeY - centerY;
              const angle = Math.atan2(dy, dx);

              // Find point on the main node's perimeter in the direction of the other node
              if (flow.source === "_main") {
                mainNodeX = centerX + Math.cos(angle) * mainNodeRadius;
                mainNodeY = centerY + Math.sin(angle) * mainNodeRadius;
              } else if (flow.target === "_main") {
                mainNodeX = centerX + Math.cos(angle) * mainNodeRadius;
                mainNodeY = centerY + Math.sin(angle) * mainNodeRadius;
              }

              // Update source and target points based on which is the main node
              const updatedSourceX =
                flow.source === "_main" ? mainNodeX : sourceX;
              const updatedSourceY =
                flow.source === "_main" ? mainNodeY : sourceY;
              const updatedTargetX =
                flow.target === "_main" ? mainNodeX : targetX;
              const updatedTargetY =
                flow.target === "_main" ? mainNodeY : targetY;

              // Calculate control points for the curve - using the same logic as the edge path
              const curveStrength = 0.5;
              const midY =
                updatedSourceY + (updatedTargetY - updatedSourceY) * 0.5;

              // Create perpendicular offset for curve
              const perpY = (updatedTargetX - updatedSourceX) * curveStrength;

              // Apply the same offset as the edge
              const perpLength = Math.sqrt(perpY * perpY + perpY * perpY);
              const normalizedPerpY = perpLength > 0 ? perpY / perpLength : 0;
              const cp1y = midY + perpY + normalizedPerpY * edgeOffset;

              // Position label at a point ~60% along the Bezier curve
              // Using the Bezier formula to find a point along the curve
              const t = 0.6; // Parameter between 0 and 1
              const labelY =
                Math.pow(1 - t, 3) * updatedSourceY +
                3 * Math.pow(1 - t, 2) * t * cp1y +
                3 * (1 - t) * Math.pow(t, 2) * cp1y +
                Math.pow(t, 3) * updatedTargetY -
                5; // Small offset

              return labelY;
            } else {
              // For other connections, use midpoint with offset
              const midY = (sourceY + targetY) / 2;

              // Apply offset for multiple edges
              const perpY = targetX - sourceX;
              const perpLength = Math.sqrt(perpY * perpY + perpY * perpY);
              const normalizedPerpY = perpLength > 0 ? perpY / perpLength : 0;

              return midY + normalizedPerpY * edgeOffset - 5;
            }
          });
      }

      // Add arrowhead markers
      svg
        .append("defs")
        .selectAll("marker")
        .data(["arrowhead", "redarrowhead"])
        .enter()
        .append("marker")
        .attr("id", (d) => d)
        .attr("viewBox", "0 0 10 10")
        .attr("refX", 9)
        .attr("refY", 5)
        .attr("markerWidth", 6)
        .attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0 0 L 10 5 L 0 10 z")
        .attr("fill", (d) => (d === "arrowhead" ? "#000" : "#f5222d"));

      // Step 5: Set up the initial view to fit everything
      // Calculate required scale to see all nodes
      const contentWidth = radius * 2 + 60 * 2;
      const contentHeight = radius * 2 + 60 * 2;
      const scaleX = svgWidth / contentWidth;
      const scaleY = svgHeight / contentHeight;
      const finalScale = Math.min(scaleX, scaleY, 1) * 0.9;

      if (previousScaleRef.current === null) {
        // Update the zoom behavior to allow better navigation
        zoom
          .scaleExtent([0.1, 2]) // Allow zooming from 0.1x to 2x
          .on("zoom", (event) => {
            inner.attr("transform", event.transform);
          });
        svg.call(
          zoom.transform,
          d3.zoomIdentity
            .translate(centerX, centerY)
            .scale(finalScale)
            .translate(-centerX, -centerY),
        ); // Double translate to ensure centering
      }

      // Set viewBox to contain the graph
      svg.attr("viewBox", `0 0 ${svgWidth} ${svgHeight}`);

      // After rendering all elements, add explicit ID attributes
      mainContainer.selectAll("g.node").attr("id", (d) => d as string);
      mainContainer.selectAll("g.cluster").attr("id", (d) => d as string);

      // Function to highlight edges related to a node
      const highlightRelatedEdges = (nodeId: string) => {
        const svg = d3.select(svgRef.current);
        const mainContainer = svg.select("g > g"); // Get the main container

        // Check if the selected node is an actor
        const selectedNodeData = g.node(nodeId);
        const isActor = selectedNodeData && selectedNodeData.class === "actor";

        // If it's an actor, find all its methods
        const relatedNodeIds = [nodeId];
        if (isActor) {
          graphData.methods.forEach((method) => {
            if (method.actorId === nodeId) {
              relatedNodeIds.push(method.id);
            }
          });
        }

        // Reset all edges to transparent first
        // For main container edges
        mainContainer.selectAll("path.edge").style("opacity", 0.1);
        // For subgraph edges
        mainContainer
          .selectAll("g > g > g.edgePath path")
          .style("opacity", 0.1);

        // Reset all labels to transparent
        // For main container labels
        mainContainer.selectAll(".edge-label").style("opacity", 0.1);
        // For subgraph labels
        mainContainer.selectAll("g > g > g.edgeLabel").style("opacity", 0.1);

        // Highlight edges connected to the selected node or its methods
        // First, highlight edges in the main container
        mainContainer.selectAll("path.edge").each(function () {
          const edge = d3.select(this);
          const source = edge.attr("data-source");
          const target = edge.attr("data-target");

          let isConnected = false;
          for (const id of relatedNodeIds) {
            if (
              (source && (source === id || source.includes(id))) ||
              (target && (target === id || target.includes(id)))
            ) {
              isConnected = true;
              break;
            }
          }

          if (isConnected) {
            edge.style("opacity", 1);

            // Find and highlight the corresponding label
            if (source && target) {
              mainContainer.selectAll(".edge-label").each(function () {
                const label = d3.select(this);
                const labelX = parseFloat(label.attr("x") || "0");
                const labelY = parseFloat(label.attr("y") || "0");

                const pathElement = edge.node() as SVGPathElement;
                if (pathElement && pathElement.getTotalLength) {
                  const pathLength = pathElement.getTotalLength();
                  const midPoint = pathElement.getPointAtLength(pathLength / 2);

                  const dx = midPoint.x - labelX;
                  const dy = midPoint.y - labelY;
                  const distance = Math.sqrt(dx * dx + dy * dy);

                  if (distance < 30) {
                    label.style("opacity", 1);
                  }
                }
              });
            }
          }
        });

        // Then, highlight edges in subgraphs
        mainContainer.selectAll("g > g > g.edgePath path").each(function () {
          const edgePath = d3.select(this);
          const edgeData = d3.select(this).datum() as any;

          if (edgeData && edgeData.v && edgeData.w) {
            let isConnected = false;
            for (const id of relatedNodeIds) {
              if (edgeData.v === id || edgeData.w === id) {
                isConnected = true;
                break;
              }
            }

            if (isConnected) {
              // Highlight the edge path
              edgePath.style("opacity", 1);

              // Find and highlight the corresponding label
              mainContainer.selectAll("g.edgeLabel").each(function () {
                const labelData = d3.select(this).datum() as any;
                if (
                  labelData &&
                  labelData.w === edgeData.w &&
                  labelData.v === edgeData.v
                ) {
                  d3.select(this).style("opacity", 1);
                }
              });
            }
          }
        });
      };

      // Add click handler to the background to reset edge highlighting
      svg.on("click", () => {
        const mainContainer = svg.select("g > g");
        // Reset all edges to full opacity
        mainContainer.selectAll("path.edge").style("opacity", 1);
        mainContainer.selectAll("g > g > g.edgePath path").style("opacity", 1);

        // Reset all labels to full opacity
        mainContainer.selectAll(".edge-label").style("opacity", 1);
        mainContainer.selectAll("g > g > g.edgeLabel").style("opacity", 1);
      });

      // Add back the click handler for nodes
      mainContainer
        .selectAll("g.node, g.cluster")
        .style("pointer-events", "all")
        .style("cursor", "pointer")
        .on("click", (event: Event) => {
          event.stopPropagation();
          const target = event.currentTarget as SVGGElement;
          const nodeId = d3.select(target).attr("id");
          if (!nodeId) {
            return;
          }

          // Highlight edges related to this node
          highlightRelatedEdges(nodeId);

          let nodeData;
          if (nodeId === "_main") {
            nodeData = { originalData: { ...mainNode, type: "function" } };
          } else {
            nodeData = g.node(nodeId);
          }
          nodeData = nodeData as any;

          if (nodeData?.originalData) {
            onElementClick({ ...nodeData.originalData });
          } else {
            console.warn("Clicked element has no original data", nodeData);
          }
        });

      if (
        previousCenterXRef.current !== null &&
        previousCenterYRef.current !== null &&
        previousScaleRef.current !== null &&
        !isNaN(previousCenterXRef.current) &&
        !isNaN(previousCenterYRef.current) &&
        !isNaN(previousScaleRef.current)
      ) {
        inner.attr(
          "transform",
          `translate(${previousCenterXRef.current},${previousCenterYRef.current}) scale(${previousScaleRef.current})`,
        );
      }
      if (
        previousCenterXRef.current === null ||
        previousCenterYRef.current === null ||
        previousScaleRef.current === null
      ) {
        updateParnetRef(inner);
      }
      // Update main node style if search is active
      const mainNodeCircle = mainContainer.select(
        ".main-node-container circle",
      );
      if (mainNodeCircle.node()) {
        const matches = nodeMatchesSearch(mainNode);
        mainNodeCircle.style("opacity", searchTerm && !matches ? 0.3 : 1);
      }
    };

    const updateParnetRef = (inner: any) => {
      if (inner && !inner.empty()) {
        const innerTransform = inner.attr("transform");
        const match = innerTransform?.match(
          /translate\(([^,]+),([^)]+)\) scale\(([^)]+)\)/,
        );

        if (match) {
          // Store the transform values - this is what we need to track
          const x = parseFloat(match[1]);
          const y = parseFloat(match[2]);
          const scale = parseFloat(match[3]);

          // Only set values if they are valid numbers
          if (!isNaN(x) && !isNaN(y) && !isNaN(scale)) {
            previousCenterXRef.current = x;
            previousCenterYRef.current = y;
            previousScaleRef.current = scale;
          }
        }
      }
    };

    useEffect(() => {
      if (svgRef.current) {
        renderGraph();
      }
      // eslint-disable-next-line
    }, [graphData, searchTerm, activeDebugSessions]);

    return (
      <div ref={containerRef} className="ray-visualization-container">
        <div className="graph-container">
          <svg ref={svgRef} width="100%" height="600"></svg>
        </div>
      </div>
    );
  },
);

RayVisualization.defaultProps = {
  showInfoCard: true,
};

export default RayVisualization;
