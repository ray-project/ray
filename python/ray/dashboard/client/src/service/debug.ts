import { get } from "./requestHandlers";

export type DebugSession = {
  className: string;
  funcName: string;
  taskId: string;
};

export type Breakpoint = {
  sourceFile: string;
  line: number;
};

export type BreakpointsRsp = {
  result: boolean;
  msg: string;
  data: {
    result: Breakpoint[];
  };
};

export type SetBreakpointsRsp = {
  result: boolean;
  msg: string;
};

export type DebugSessionsRsp = {
  result: boolean;
  msg: string;
  data: {
    result: DebugSession[];
  };
};

export type ActiveDebugSessionsRsp = {
  result: boolean;
  msg: string;
  data: {
    result: string[];
  };
};

export type DebugCommandRsp = {
  result: boolean;
  msg: string;
  data: {
    result: any;
  };
};

export type DebugStatusRsp = {
  result: boolean;
  msg: string;
};

export type DebugEnabledRsp = {
  result: boolean;
  msg: string;
  data: {
    result: boolean;
  };
};

/**
 * Retrieves available debug sessions for a specific job, class, and function
 */
export const getDebugSessions = async (
  jobId: string,
  className?: string | null,
  funcName?: string | null,
  filterActive = false,
): Promise<DebugSession[]> => {
  let path = `get_debug_sessions?job_id=${jobId}&filter_active=${filterActive}`;
  if (className !== null && className !== undefined && className !== "") {
    path += `&class_name=${className}`;
  }
  if (funcName !== null) {
    path += `&func_name=${funcName}`;
  }
  const result = await get<DebugSessionsRsp>(path);
  return result.data.data.result;
};

/**
 * Retrieves breakpoints for a specific task
 */
export const getBreakpoints = async (
  jobId: string,
  taskId: string,
): Promise<Breakpoint[]> => {
  const path = `get_breakpoints?job_id=${jobId}&task_id=${taskId}`;
  const result = await get<BreakpointsRsp>(path);
  return result.data.data.result;
};

/**
 * Store breakpoints
 */
export const storeBreakpoints = async (
  jobId: string,
  taskId: string,
  breakpoints: Breakpoint[],
): Promise<boolean> => {
  const breakpointsBase64 = btoa(JSON.stringify(breakpoints));
  const path = `set_breakpoints?job_id=${jobId}&task_id=${taskId}&breakpoints=${breakpointsBase64}`;
  const result = await get<SetBreakpointsRsp>(path);
  return result.data.result;
};

/**
 * Activates a debug session for a specific task
 */
export const activateDebugSession = async (
  jobId: string,
  className: string,
  funcName: string,
  taskId: string,
): Promise<boolean> => {
  let path = `activate_debug_session?job_id=${jobId}&func_name=${funcName}&task_id=${taskId}`;
  if (className !== null && className !== undefined && className !== "") {
    path += `&class_name=${className}`;
  }
  const result = await get<DebugStatusRsp>(path);
  return result.data.result;
};

/**
 * Deactivates a debug session
 */
export const deactivateDebugSession = async (
  jobId: string,
  taskId: string,
): Promise<boolean> => {
  const path = `deactivate_debug_session?job_id=${jobId}&task_id=${taskId}`;
  const result = await get<DebugStatusRsp>(path);
  return result.data.result;
};

/**
 * Retrieves active debug sessions for a job
 */
export const getActiveDebugSessions = async (
  jobId: string,
): Promise<string[]> => {
  const path = `get_active_debug_sessions?job_id=${jobId}`;
  const result = await get<ActiveDebugSessionsRsp>(path);
  return result.data.data.result;
};

/**
 * Sends a debug command to a running debug session
 */
export const sendDebugCommand = async (
  jobId: string,
  taskId: string,
  command: string,
  args: Record<string, any>,
): Promise<any> => {
  // Encode args as base64 to avoid URL encoding issues
  const argsBase64 = btoa(JSON.stringify(args));
  const path = `debug_cmd?job_id=${jobId}&task_id=${taskId}&command=${command}&args=${argsBase64}`;
  const result = await get<DebugCommandRsp>(path);
  return result.data.data.result;
};

/**
 * Retrieves the debug enabled status for a job
 */
export const getDebugEnabled = async (jobId: string): Promise<boolean> => {
  const path = `get_debug_enabled?job_id=${jobId}`;
  const result = await get<DebugEnabledRsp>(path);
  return result.data.data.result;
};

/**
 * Sets the debug enabled status for a job
 */
export const setDebugEnabled = async (
  jobId: string,
  enabled: boolean,
): Promise<boolean> => {
  const path = `set_debug_enabled?job_id=${jobId}&debug_enabled=${enabled}`;
  const result = await get<DebugStatusRsp>(path);
  return result.data.result;
};
