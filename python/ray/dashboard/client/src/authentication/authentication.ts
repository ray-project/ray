/**
 * Authentication service for Ray dashboard.
 * Provides functions to check authentication mode and authenticate with tokens.
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
 * Authenticate with the server using a token.
 * If the token is valid, the server will set an HttpOnly cookie that will be
 * automatically included in all subsequent requests.
 *
 * @param token - The authentication token to validate and use
 * @returns Promise resolving to true if authentication succeeded, false otherwise
 */
export const authenticateWithToken = async (
  token: string,
): Promise<boolean> => {
  try {
    await axios.post(
      formatUrl("/api/authenticate"),
      {},
      {
        headers: { Authorization: `Bearer ${token}` },
      },
    );
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
