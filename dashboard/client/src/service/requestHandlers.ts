import axios, { AxiosResponse, AxiosRequestConfig } from "axios";

const unformattedPrefix = (process.env.REACT_APP_API_PREFIX || "").trim();

const getPrefix = (): string => {
  if (unformattedPrefix[unformattedPrefix.length - 1] === "/") {
    return unformattedPrefix.slice(0, unformattedPrefix.length - 1);
  }
  return unformattedPrefix;
};

export const formatUrl = (url: string): string => {
  const prefix = getPrefix();

  if (url.startsWith("/")) {
    return `${prefix}${url}`;
  }
  return `${prefix}/${url}`;
};

export function get<T = any, R = AxiosResponse<T>>(url: string, config?: AxiosRequestConfig): Promise<R> {
  return axios.get<T, R>(formatUrl(url), config);
}
