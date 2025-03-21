/**
 * This utility file formats and sends HTTP requests such that
 * they fullfill the requirements expected by users of the dashboard.
 *
 * All HTTP requests should be sent using the helpers in this file.
 *
 * More HTTP Methods helpers should be added to this file when the need
 * arises.
 */

import axios, { AxiosRequestConfig, AxiosResponse } from "axios";

/**
 * This function formats URLs such that the user's browser
 * sets the HTTP request's Request URL relative to the path at
 * which the dashboard is served.
 * This works behind a reverse proxy.
 *
 * @param {String} url The URL to be hit
 * @return {String}    The reverse proxy compatible URL
 */

const axiosInstance = axios.create();

export const formatUrl = (url: string): string => {
  if (url.startsWith("/")) {
    return url.slice(1);
  }
  return url;
};

export const get = <T = any, R = AxiosResponse<T>>(
  url: string,
  config?: AxiosRequestConfig,
): Promise<R> => {
  return axiosInstance.get<T, R>(formatUrl(url), config);
};

export const post = <T = any, R = AxiosResponse<T>>(
  url: string,
  data?: any,
  config?: AxiosRequestConfig,
): Promise<R> => {
  console.log(
    `URL posted: ${formatUrl(url)} - data  ${data} - config  ${config}`,
  );
  return axiosInstance.post<T, R>(formatUrl(url), data, config);
};

export const deleteRequest = <T = any, R = AxiosResponse<T>>(
  url: string,
  data?: any,
  config?: AxiosRequestConfig,
): Promise<R> => {
  console.log(
    `URL posted: ${formatUrl(url)} - data  ${data} - config  ${config}`,
  );
  return axiosInstance.delete<T, R>(formatUrl(url), data);
};
