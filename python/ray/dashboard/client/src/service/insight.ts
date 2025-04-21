import { encodingForModel, TiktokenModel } from "js-tiktoken";
import { get } from "./requestHandlers";

export type InsightAnalyzePromptRsp = {
  result: boolean;
  msg: string;
  data: {
    prompt: string;
  };
};

/**
 * Calculates the number of tokens in a string using js-tiktoken
 */
export const countTokens = (text: string, model = "gpt-4"): number => {
  try {
    // Get the encoding for the model
    const enc = encodingForModel(model.split("/")[1] as TiktokenModel);

    // Encode and count tokens
    const tokens = enc.encode(text);
    const count = tokens.length;

    return count;
  } catch (error) {
    console.error("Error counting tokens:", error);
    // Fallback: estimate tokens as ~4 characters per token
    return Math.ceil(text.length / 4);
  }
};

/**
 * Retrieves the prompt for insight analysis
 */
export const getInsightAnalyzePrompt = async (
  jobId: string,
): Promise<string> => {
  const path = `get_insight_analyze_prompt?job_id=${jobId}`;
  const result = await get<InsightAnalyzePromptRsp>(path);
  return result.data.data.prompt;
};

/**
 * Call OpenAI API to generate a report
 */
export const generateReport = async (
  prompt: string,
  apiKey: string,
  baseUrl: string,
  model: string,
  maxTokens?: number,
  stream?: boolean,
  onChunk?: (chunk: string) => void,
): Promise<any> => {
  try {
    const requestBody: any = {
      model: model,
      messages: [
        {
          role: "user",
          content: prompt,
        },
      ],
      temperature: 0.7,
    };

    // Add max_tokens if provided
    if (maxTokens) {
      requestBody.max_tokens = Math.min(maxTokens, 4096);
    }

    // Add streaming if requested
    if (stream) {
      requestBody.stream = true;
    }

    const response = await fetch(`${baseUrl}/v1/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      throw new Error(`API request failed with status ${response.status}`);
    }

    // Handle streaming response
    if (stream && onChunk) {
      const reader = response.body?.getReader();
      if (!reader) {
        throw new Error("Response body is not readable");
      }

      let content = "";
      const decoder = new TextDecoder();

      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }

        const chunk = decoder.decode(value);
        const lines = chunk.split("\n").filter((line) => line.trim() !== "");

        for (const line of lines) {
          if (line.startsWith("data: ")) {
            const data = line.substring(6);
            if (data === "[DONE]") {
              continue;
            }

            try {
              const parsed = JSON.parse(data);
              const contentDelta = parsed.choices[0]?.delta?.content;
              if (contentDelta) {
                content += contentDelta;
                onChunk(contentDelta);
              }
            } catch (e) {
              console.error("Error parsing SSE data:", e);
            }
          }
        }
      }

      return content;
    }

    // Handle non-streaming response
    const data = await response.json();
    try {
      const content = data.choices[0].message.content;
      return content;
    } catch (error) {
      console.error("Failed to parse JSON response:", error);
      return {
        error: "Failed to parse response",
        raw: data.choices[0].message.content,
      };
    }
  } catch (error) {
    console.error("Error calling OpenAI API:", error);
    throw error;
  }
};

// Helper function to convert timestamps to readable format
export const formatTimestamp = (timestamp: number): string => {
  return new Date(timestamp).toLocaleString();
};

// Helper function to convert any timestamps in an object to readable format
export const convertTimestampsInObject = (obj: any): any => {
  if (!obj) {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map((item) => convertTimestampsInObject(item));
  } else if (typeof obj === "object") {
    const result: any = {};
    for (const key in obj) {
      if (
        (key === "timestamp" || key === "startTime") &&
        typeof obj[key] === "number"
      ) {
        result[key] = formatTimestamp(obj[key]);
      } else {
        result[key] = convertTimestampsInObject(obj[key]);
      }
    }
    return result;
  }

  return obj;
};

// Helper function to replace IDs with actual names in graph data
export const replaceIdsWithNames = (graphData: any): any => {
  if (!graphData) {
    return graphData;
  }

  const result = { ...graphData };

  // Create a mapping of IDs to names
  const idToNameMap: Record<string, string> = {};

  // First pass: collect all actor, method, and function IDs and their names
  if (result.actors) {
    result.actors.forEach((actor: any) => {
      if (actor.id && actor.name) {
        idToNameMap[actor.id] = actor.name;
      }
    });
  }

  if (result.methods) {
    result.methods.forEach((method: any) => {
      if (method.id && method.name) {
        idToNameMap[method.id] = method.name;
      }
    });
  }

  if (result.functions) {
    result.functions.forEach((func: any) => {
      if (func.id && func.name) {
        idToNameMap[func.id] = func.name;
      }
    });
  }

  // Second pass: replace IDs with names in callFlows and dataFlows
  if (result.callFlows) {
    result.callFlows = result.callFlows.map((flow: any) => {
      const updatedFlow = { ...flow };

      // Add source name if available
      if (flow.source && idToNameMap[flow.source]) {
        updatedFlow.source = idToNameMap[flow.source];
      }

      // Add target name if available
      if (flow.target && idToNameMap[flow.target]) {
        updatedFlow.target = idToNameMap[flow.target];
      }

      return updatedFlow;
    });
  }

  if (result.dataFlows) {
    result.dataFlows = result.dataFlows.map((flow: any) => {
      const updatedFlow = { ...flow };

      // Add source name if available
      if (flow.source && idToNameMap[flow.source]) {
        updatedFlow.source = idToNameMap[flow.source];
      }

      // Add target name if available
      if (flow.target && idToNameMap[flow.target]) {
        updatedFlow.target = idToNameMap[flow.target];
      }

      return updatedFlow;
    });
  }

  return result;
};
