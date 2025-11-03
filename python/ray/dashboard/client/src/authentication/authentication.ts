/**
 * Authentication service for Ray dashboard.
 * Provides functions to check authentication mode and validate tokens when token auth is enabled.
 */

import axios from "axios";
import { formatUrl, get } from "../service/requestHandlers";

/**
 * Response type for authentication mode endpoint.
 */
export type AuthenticationModeResponse = {
  authentication_mode: "disabled" | "token";
};

/**
 * Get the current authentication mode from the server.
 * This endpoint is public and does not require authentication.
 *
 * @returns Promise resolving to the authentication mode
 */
export const getAuthenticationMode =
  async (): Promise<AuthenticationModeResponse> => {
    const response = await get<AuthenticationModeResponse>(
      "/api/authentication_mode",
    );
    return response.data;
  };

/**
 * Test if a token is valid by making a request to the /api/version endpoint
 * which is fast and reliable.
 *
 * Note: This uses plain axios (not axiosInstance) to avoid the request interceptor
 * that would add the token from cookies, since we want to test the specific token
 * passed as a parameter. It also avoids the response interceptor that would dispatch
 * global authentication error events, since we handle 401/403 errors locally.
 *
 * @param token - The authentication token to test
 * @returns Promise resolving to true if token is valid, false otherwise
 */
export const testTokenValidity = async (token: string): Promise<boolean> => {
  try {
    await axios.get(formatUrl("/api/version"), {
      headers: { Authorization: `Bearer ${token}` },
    });
    return true;
  } catch (error: any) {
    // 401 (Unauthorized) or 403 (Forbidden) means invalid token
    if (error.response?.status === 401 || error.response?.status === 403) {
      return false;
    }
    // For other errors (network, server errors, etc.), re-throw
    throw error;
  }
};
