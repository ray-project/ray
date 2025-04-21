import DownloadIcon from "@mui/icons-material/Download";
import RefreshIcon from "@mui/icons-material/Refresh";
import SettingsIcon from "@mui/icons-material/Settings";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  TextField,
  Typography,
} from "@mui/material";
import mermaid from "mermaid";
import React, { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { materialLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import remarkGfm from "remark-gfm";
import {
  convertTimestampsInObject,
  countTokens,
  generateReport,
  getInsightAnalyzePrompt,
  replaceIdsWithNames,
} from "../service/insight";

// Initialize mermaid
mermaid.initialize({
  startOnLoad: true,
  theme: "default",
  securityLevel: "loose",
  fontFamily: "Verdana, sans-serif",
  suppressErrorRendering: true,
});

type InsightPanelProps = {
  jobId?: string;
  graphData: any;
  physicalViewData: any;
  flameData: any;
};

// Custom markdown components for better rendering
const MarkdownComponents = {
  // eslint-disable-next-line
  code({ node, inline, className, children, ...props }: any) {
    const match = /language-(\w+)/.exec(className || "");
    const language = match && match[1] ? match[1] : "";

    // Handle Mermaid diagrams specially
    if (language === "mermaid") {
      return <MermaidDiagram chart={String(children)} />;
    }

    return !inline && language ? (
      <SyntaxHighlighter
        style={materialLight}
        language={language}
        PreTag="div"
        {...props}
      >
        {String(children).replace(/\n$/, "")}
      </SyntaxHighlighter>
    ) : (
      <code className={className} {...props}>
        {children}
      </code>
    );
  },
  // eslint-disable-next-line
  table({ children, ...props }: any) {
    return (
      <Box sx={{ overflowX: "auto", my: 2 }}>
        <table style={{ borderCollapse: "collapse", width: "100%" }} {...props}>
          {children}
        </table>
      </Box>
    );
  },
  // eslint-disable-next-line
  th({ children, ...props }: any) {
    return (
      <th
        style={{
          textAlign: "left",
          padding: "8px",
          borderBottom: "1px solid #ddd",
        }}
        {...props}
      >
        {children}
      </th>
    );
  },
  // eslint-disable-next-line
  td({ children, ...props }: any) {
    return (
      <td style={{ padding: "8px", borderBottom: "1px solid #ddd" }} {...props}>
        {children}
      </td>
    );
  },
  // eslint-disable-next-line
  a({ children, ...props }: any) {
    return (
      <a style={{ color: "#1976d2", textDecoration: "none" }} {...props}>
        {children}
      </a>
    );
  },
  // eslint-disable-next-line
  img({ src, alt, ...props }: any) {
    return (
      <Box sx={{ textAlign: "center", my: 2 }}>
        <img src={src} alt={alt} style={{ maxWidth: "100%" }} {...props} />
      </Box>
    );
  },
};

// Mermaid diagram component
const MermaidDiagram = ({ chart }: { chart: string }) => {
  const ref = useRef<HTMLDivElement>(null);
  const [svg, setSvg] = useState<string>("");

  useEffect(() => {
    if (ref.current && chart && chart.trim()) {
      try {
        // Add a timeout to prevent infinite rendering loops
        const renderTimeout = setTimeout(() => {
          try {
            const uniqueId = `mermaid-${Date.now()}`;
            mermaid
              .render(uniqueId, chart.trim())
              .then(({ svg }) => {
                if (svg && svg.trim()) {
                  setSvg(svg);
                } else {
                  console.warn("Empty SVG returned from Mermaid");
                }
              })
              .catch((renderError) => {
                console.warn("Error rendering Mermaid diagram:", renderError);
              });
          } catch (error) {
            console.warn("Exception during Mermaid rendering:", error);
          }
        }, 0);

        return () => clearTimeout(renderTimeout);
      } catch (error) {
        console.warn("Error initializing Mermaid diagram:", error);
      }
    }
  }, [chart]);
  return (
    <div
      ref={ref}
      className="mermaid-diagram"
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
};

// Utility function to chunk data based on token count
const chunkData = (data: any, maxTokens = 10000, model = "gpt-4"): string[] => {
  const dataStr = JSON.stringify(data, null, 2);
  const totalTokens = countTokens(dataStr, model);
  const chunks: string[] = [];

  // If data is small enough, return it as a single chunk
  if (totalTokens <= maxTokens) {
    return [dataStr];
  }

  // Calculate how many chunks we need
  const numChunks = Math.ceil(totalTokens / maxTokens);

  try {
    // Calculate the basic chunk size without overlap
    const basicChunkSize = Math.ceil(dataStr.length / numChunks);
    // Calculate the size of each chunk with overlap
    const overlapSize = Math.ceil(basicChunkSize / 5); // 1/5 overlap

    for (let i = 0; i < numChunks; i++) {
      // For first chunk, start at 0, otherwise start 1/5 chunk earlier
      const start = i === 0 ? 0 : Math.max(0, i * basicChunkSize - overlapSize);
      const end = Math.min((i + 1) * basicChunkSize, dataStr.length);
      chunks.push(dataStr.substring(start, end));
    }
  } catch (error) {
    console.error("Error chunking data:", error);
    // Fallback to simple character-based chunking with overlap
    const basicChunkSize = Math.ceil(dataStr.length / numChunks);
    const overlapSize = Math.ceil(basicChunkSize / 5); // 1/5 overlap

    for (let i = 0; i < numChunks; i++) {
      const start = i === 0 ? 0 : Math.max(0, i * basicChunkSize - overlapSize);
      const end = Math.min((i + 1) * basicChunkSize, dataStr.length);
      chunks.push(dataStr.substring(start, end));
    }
  }

  return chunks;
};

const InsightPanel: React.FC<InsightPanelProps> = ({
  jobId,
  graphData,
  physicalViewData,
  flameData,
}) => {
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [showSettings, setShowSettings] = useState<boolean>(false);
  const [report, setReport] = useState<string | null>(() => {
    // Initialize from localStorage if available for this job
    if (jobId) {
      const savedReport = localStorage.getItem(`insight-report-${jobId}`);
      return savedReport || null;
    }
    return null;
  });
  const [reportStream, setReportStream] = useState<string | null>(null);
  const [isStreaming, setIsStreaming] = useState<boolean>(false);

  // Settings state
  const [openaiBaseUrl, setOpenaiBaseUrl] = useState<string>(
    localStorage.getItem("openaiBaseUrl") || "",
  );
  const [openaiApiKey, setOpenaiApiKey] = useState<string>(
    localStorage.getItem("openaiApiKey") || "",
  );
  const [openaiModel, setOpenaiModel] = useState<string>(
    localStorage.getItem("openaiModel") || "",
  );
  const [contextLength, setContextLength] = useState<string>(
    localStorage.getItem("contextLength") || "64000",
  );
  const [reportLanguage, setReportLanguage] = useState<string>(
    localStorage.getItem("reportLanguage") || "English",
  );

  // Save settings to localStorage whenever they change
  useEffect(() => {
    localStorage.setItem("openaiBaseUrl", openaiBaseUrl);
    localStorage.setItem("openaiApiKey", openaiApiKey);
    localStorage.setItem("openaiModel", openaiModel);
    localStorage.setItem("contextLength", contextLength);
    localStorage.setItem("reportLanguage", reportLanguage);
  }, [openaiBaseUrl, openaiApiKey, openaiModel, contextLength, reportLanguage]);

  // Save report to localStorage whenever it changes
  useEffect(() => {
    if (jobId && report) {
      localStorage.setItem(`insight-report-${jobId}`, report);
    }
  }, [jobId, report]);

  // Function to export the report as markdown
  const exportReport = () => {
    if (!report && !reportStream) {
      return;
    }

    const content = isStreaming ? reportStream : report;
    if (!content) {
      return;
    }

    const blob = new Blob([content], { type: "text/markdown" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `insight-report-${jobId || "unknown"}-${
      new Date().toISOString().split("T")[0]
    }.md`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const generateInsightReport = async () => {
    if (!jobId) {
      setError("No job ID provided");
      return;
    }

    if (!openaiApiKey) {
      setError("OpenAI API key is required");
      return;
    }

    setLoading(true);
    setError(null);
    setReport(null);
    setReportStream(null);
    setIsStreaming(false);

    try {
      // 1. Get the analyze promp
      const prompt = await getInsightAnalyzePrompt(jobId);
      const modelToUse = openaiModel || "gpt-4";
      const modelContextLength = parseInt(contextLength) || 64000;

      // Add language instruction to the prompt
      const promptWithLanguage = reportLanguage
        ? `${prompt}\n\nPlease generate the report in ${reportLanguage}.
        Just give result markdown without any other text, wrap it with \`\`\`markdown.
        For example:
        \`\`\`markdown
        # Analysis Report
        ...
        \`\`\`
        `
        : prompt;

      // 2. Prepare all data with readable timestamps and names
      const prepareData = (data: any) => {
        if (!data) {
          return null;
        }
        const dataWithReadableTime = convertTimestampsInObject(data);
        return replaceIdsWithNames(dataWithReadableTime);
      };

      const preparedGraphData = prepareData(graphData);
      const preparedPhysicalData = prepareData(physicalViewData);
      const preparedFlameData = prepareData(flameData);

      // 3. Combine all available data into a single object
      const combinedData = {
        graphData: preparedGraphData,
        physicalViewData: preparedPhysicalData,
        flameData: preparedFlameData,
      };

      // 4. Chunk the combined data using the user-defined context length
      const dataChunks = chunkData(
        combinedData,
        Math.floor((modelContextLength * 5) / 6),
        modelToUse,
      );

      // Calculate max tokens per chunk based on the context length
      const maxTokensPerChunk = Math.floor(
        Math.floor((modelContextLength * 5) / 6) / dataChunks.length,
      );

      // Count tokens in the prompt to ensure it doesn't exceed limits
      const promptTokens = countTokens(promptWithLanguage, modelToUse);
      console.log(
        `Prompt tokens: ${promptTokens}, available per chunk: ${maxTokensPerChunk}`,
      );

      // 5. Generate reports for each chunk concurrently
      const chunkPrompts = dataChunks.map((chunk, index) => {
        return `${promptWithLanguage}\n\nChunk ${index + 1} of ${
          dataChunks.length
        }:\n${chunk}`;
      });

      // If there's only one chunk, use streaming directly to avoid generating the report twice
      if (dataChunks.length === 1) {
        setIsStreaming(true);
        let streamedContent = "";
        let found_markdown_marker = false;

        await generateReport(
          chunkPrompts[0],
          openaiApiKey,
          openaiBaseUrl,
          modelToUse,
          maxTokensPerChunk,
          true, // Enable streaming
          (chunk) => {
            streamedContent += chunk;
            if (!found_markdown_marker) {
              const pos = streamedContent.indexOf("```markdown");
              if (pos !== -1) {
                found_markdown_marker = true;
                const cleanedContent = streamedContent.slice(
                  pos + "```markdown".length,
                );
                streamedContent = cleanedContent;
                setReportStream(streamedContent);
              }
            } else {
              setReportStream(streamedContent);
            }
          },
        );

        // Clean the final content when streaming is complete
        const finalContent = cleanMarkdown(
          streamedContent,
          found_markdown_marker,
        );
        setReport(finalContent);
        setIsStreaming(false);
        setShowSettings(false);
        return;
      }

      // Only proceed with non-streaming generation for multiple chunks
      const chunkReportsPromises = chunkPrompts.map((chunkPrompt) =>
        generateReport(
          chunkPrompt,
          openaiApiKey,
          openaiBaseUrl,
          modelToUse,
          maxTokensPerChunk,
        ),
      );

      const chunkReports = await Promise.all(chunkReportsPromises);

      // 7. Merge all chunk reports into a final report with streaming
      const mergePrompt = `
        You are given multiple analysis reports of the same data, each covering different chunks.
        Please merge these reports into a cohesive, comprehensive analysis, eliminating redundancies
        and ensuring consistent insights.
        
        ${chunkReports
          .map((report, index) => `Report ${index + 1}:\n${report}`)
          .join("\n\n")}
        
        Return the merged analysis in markdown format.
        Just give result markdown without any other text, wrap it with \`\`\`markdown.
        ${
          reportLanguage
            ? `\nPlease write the report in ${reportLanguage}.`
            : ""
        }
        
        for example:
        \`\`\`markdown
        # Analysis Report
        ...
        \`\`\`
      `;

      // Count tokens for merge prompt
      const mergePromptTokens = countTokens(mergePrompt, modelToUse);
      console.log(
        `Merge prompt tokens: ${mergePromptTokens}, max output: ${maxTokensPerChunk}`,
      );

      setIsStreaming(true);
      let streamedContent = "";
      let found_markdown_marker = false;

      await generateReport(
        mergePrompt,
        openaiApiKey,
        openaiBaseUrl,
        modelToUse,
        maxTokensPerChunk,
        true, // Enable streaming
        (chunk) => {
          streamedContent += chunk;
          // Still try to find markdown marker and clean up if found
          if (!found_markdown_marker) {
            const pos = streamedContent.indexOf("```markdown");
            if (pos !== -1) {
              found_markdown_marker = true;
              const cleanedContent = streamedContent.slice(
                pos + "```markdown".length,
              );
              streamedContent = cleanedContent;
              setReportStream(streamedContent);
            }
          } else {
            setReportStream(streamedContent);
          }
        },
      );

      // Clean the final content when streaming is complete
      const finalContent = cleanMarkdown(
        streamedContent,
        found_markdown_marker,
      );
      setReport(finalContent);
      setIsStreaming(false);
      setShowSettings(false);
    } catch (error) {
      console.error("Error generating report:", error);
      setError(
        error instanceof Error ? error.message : "Unknown error occurred",
      );
      setIsStreaming(false);
    } finally {
      setLoading(false);
    }
  };

  // Helper function to clean up markdown output
  const cleanMarkdown = (
    markdown: string,
    found_markdown_marker: boolean,
  ): string => {
    // First check for the starting markdown marker
    const markdown_marker_end = "```";
    let cleaned = markdown;

    // Remove the ending markdown marker if present
    const end_pos = cleaned.lastIndexOf(markdown_marker_end);
    if (end_pos !== -1 && found_markdown_marker) {
      cleaned = cleaned.slice(0, end_pos);
    }

    return cleaned;
  };

  // Add function to clear saved report
  const clearSavedReport = () => {
    if (jobId) {
      localStorage.removeItem(`insight-report-${jobId}`);
      setReport(null);
      setReportStream(null);
    }
  };

  const renderReport = () => {
    if (!report && !reportStream) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "300px",
            gap: 2,
            maxHeight: "calc(80vh - 90px)",
          }}
        >
          <Button
            variant="contained"
            color="primary"
            onClick={generateInsightReport}
            disabled={loading}
            startIcon={loading && <CircularProgress size={20} />}
          >
            {loading ? "Generating..." : "Generate Report"}
          </Button>
          <Button variant="text" onClick={() => setShowSettings(true)}>
            Settings
          </Button>
        </Box>
      );
    }

    const displayContent = isStreaming ? reportStream : report;

    return (
      <Box
        sx={{
          p: 2,
          overflow: "auto",
          height: "80%",
          maxHeight: "calc(80vh - 90px)",
        }}
      >
        <Box
          sx={{ display: "flex", justifyContent: "flex-end", mb: 2, gap: 1 }}
        >
          <Button
            variant="outlined"
            onClick={exportReport}
            size="small"
            startIcon={<DownloadIcon />}
            disabled={!displayContent}
          >
            Export
          </Button>
          <Button
            variant="outlined"
            onClick={generateInsightReport}
            size="small"
            disabled={loading}
            startIcon={
              loading ? <CircularProgress size={16} /> : <RefreshIcon />
            }
          >
            {loading
              ? isStreaming
                ? "Streaming..."
                : "Regenerating..."
              : "Regenerate"}
          </Button>
          <Button
            variant="outlined"
            onClick={clearSavedReport}
            size="small"
            disabled={loading || (!report && !reportStream)}
          >
            Clear
          </Button>
          <Button
            variant="text"
            onClick={() => setShowSettings(true)}
            size="small"
            startIcon={<SettingsIcon />}
          >
            Settings
          </Button>
        </Box>
        <Box
          sx={{
            "& h1, & h2, & h3, & h4, & h5, & h6": {
              mt: 2,
              mb: 1,
              fontWeight: "medium",
            },
            "& p": {
              my: 1,
              lineHeight: 1.6,
            },
            "& ul, & ol": {
              paddingLeft: 3,
            },
            "& li": {
              mb: 0.5,
            },
            "& blockquote": {
              borderLeft: "4px solid #e0e0e0",
              my: 1,
              pl: 2,
              py: 0.5,
              backgroundColor: "#f5f5f5",
            },
            "& pre": {
              mt: 1,
              mb: 2,
            },
            "& .mermaid-diagram": {
              textAlign: "center",
              margin: "16px 0",
            },
            "& .mermaid-error": {
              margin: "16px 0",
              padding: "8px",
              backgroundColor: "#f9f9f9",
              borderRadius: "4px",
              border: "1px solid #eee",
              overflow: "auto",
            },
            "& .mermaid-error pre": {
              margin: 0,
              padding: 0,
            },
            "& .mermaid-error code": {
              fontFamily: "monospace",
              fontSize: "0.9em",
              color: "#666",
            },
          }}
        >
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={MarkdownComponents}
          >
            {displayContent}
          </ReactMarkdown>
        </Box>
      </Box>
    );
  };

  const renderSettings = () => {
    return (
      <Box sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          OpenAI API Settings
        </Typography>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            gap: 2,
            maxWidth: "600px",
          }}
        >
          <TextField
            label="OpenAI API Base URL"
            value={openaiBaseUrl}
            onChange={(e) => setOpenaiBaseUrl(e.target.value)}
            fullWidth
            margin="normal"
            helperText=""
          />
          <TextField
            label="OpenAI API Key"
            value={openaiApiKey}
            onChange={(e) => setOpenaiApiKey(e.target.value)}
            fullWidth
            margin="normal"
            type="password"
          />
          <TextField
            label="OpenAI Model"
            value={openaiModel}
            onChange={(e) => setOpenaiModel(e.target.value)}
            fullWidth
            margin="normal"
            helperText=""
          />
          <TextField
            label="Model Context Length (tokens)"
            value={contextLength}
            onChange={(e) => setContextLength(e.target.value)}
            fullWidth
            margin="normal"
            type="number"
            inputProps={{ min: 1000 }}
            helperText=""
          />
          <TextField
            label="Report Language"
            value={reportLanguage}
            onChange={(e) => setReportLanguage(e.target.value)}
            fullWidth
            margin="normal"
            helperText="Specify the language for the generated report (e.g., English, Spanish, French)"
          />
          <Box sx={{ display: "flex", gap: 2, mt: 2 }}>
            <Button
              variant="contained"
              color="primary"
              onClick={() => {
                localStorage.setItem("openaiBaseUrl", openaiBaseUrl);
                localStorage.setItem("openaiApiKey", openaiApiKey);
                localStorage.setItem("openaiModel", openaiModel);
                localStorage.setItem("contextLength", contextLength);
                localStorage.setItem("reportLanguage", reportLanguage);
                setShowSettings(false);
              }}
            >
              Save & Close
            </Button>
            <Button variant="text" onClick={() => setShowSettings(false)}>
              Cancel
            </Button>
          </Box>
        </Box>
      </Box>
    );
  };

  return (
    <div
      style={{
        width: "calc(100% - 320px)",
        height: "calc(80vh - 70px)",
        position: "relative",
        backgroundColor: "transparent",
        fontFamily: "Verdana, sans-serif",
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-start",
        margin: "20px 0",
        maxWidth: "calc(100% - 320px)",
        overflow: "hidden",
      }}
    >
      {error && (
        <Alert severity="error" sx={{ m: 2, width: "100%" }}>
          {error}
        </Alert>
      )}

      <Box
        sx={{
          width: "100%",
          height: "100%",
          backgroundColor: "white",
          borderRadius: "4px",
          boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
          overflow: "auto",
        }}
      >
        {showSettings ? renderSettings() : renderReport()}
      </Box>
    </div>
  );
};

export default InsightPanel;
