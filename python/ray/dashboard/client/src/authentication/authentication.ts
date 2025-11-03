/**
 * Authentication service for Ray dashboard.
 * Provides functions to check authentication mode and validate tokens when token auth is enabled.
 */

import axios from "axios";

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
    const response = await axios.get<AuthenticationModeResponse>(
      "/api/authentication_mode",
    );
    return response.data;
  };

/**
 * Test if a token is valid by making a request to the /api/version endpoint
 * which is fast and reliable.
 *
 * @param token - The authentication token to test
 * @returns Promise resolving to true if token is valid, false otherwise
 */
export const testTokenValidity = async (token: string): Promise<boolean> => {
  try {
    await axios.get("/api/version", {
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
