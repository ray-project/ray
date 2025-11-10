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
import { AUTHENTICATION_ERROR_EVENT } from "../authentication/constants";
import { getAuthenticationToken } from "../authentication/cookies";

/**
 * This function formats URLs such that the user's browser
 * sets the HTTP request's Request URL relative to the path at
 * which the dashboard is served.
 * This works behind a reverse proxy.
 *
 * @param {String} url The URL to be hit
 * @return {String}    The reverse proxy compatible URL
 */
export const formatUrl = (url: string): string => {
  if (url.startsWith("/")) {
    return url.slice(1);
  }
  return url;
};

// Create axios instance with interceptors for authentication
const axiosInstance = axios.create();

// Export the configured axios instance for direct use when needed
export { axiosInstance };

// Request interceptor: Add authentication token if available
axiosInstance.interceptors.request.use(
  (config) => {
    const token = getAuthenticationToken();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  },
);

// Response interceptor: Handle 401/403 errors
axiosInstance.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    // If we get 401 (Unauthorized) or 403 (Forbidden), dispatch an event
    // so the App component can show the authentication dialog
    if (error.response?.status === 401 || error.response?.status === 403) {
      // Check if there was a token in the request
      const hadToken = !!getAuthenticationToken();

      // Dispatch custom event for authentication error
      window.dispatchEvent(
        new CustomEvent(AUTHENTICATION_ERROR_EVENT, {
          detail: { hadToken },
        }),
      );
    }

    // Re-throw the error so the caller can handle it if needed
    return Promise.reject(error);
  },
);

export const get = <T = any, R = AxiosResponse<T>>(
  url: string,
  config?: AxiosRequestConfig,
): Promise<R> => {
  return axiosInstance.get<T, R>(formatUrl(url), config);
};
