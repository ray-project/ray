/**
 * Dialog component for Ray dashboard token authentication.
 * Prompts users to enter their authentication token when token auth is enabled.
 */

import { Visibility, VisibilityOff } from "@mui/icons-material";
import {
  Alert,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  InputAdornment,
  TextField,
} from "@mui/material";
import React, { useState } from "react";

export type TokenAuthenticationDialogProps = {
  /** Whether the dialog is open */
  open: boolean;
  /** Whether the user has previously entered a token (affects messaging) */
  hasExistingToken: boolean;
  /** Callback when user submits a token */
  onSubmit: (token: string) => Promise<void>;
  /** Optional error message to display */
  error?: string;
};

/**
 * Token Authentication Dialog Component.
 *
 * Shows different messages based on whether this is the first time
 * (hasExistingToken=false) or if a previously stored token was rejected
 * (hasExistingToken=true).
 */
export const TokenAuthenticationDialog: React.FC<TokenAuthenticationDialogProps> =
  ({ open, hasExistingToken, onSubmit, error }) => {
    const [token, setToken] = useState("");
    const [showToken, setShowToken] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);

    const handleSubmit = async () => {
      if (!token.trim()) {
        return;
      }

      setIsSubmitting(true);
      try {
        await onSubmit(token.trim());
        // If successful, the parent component will close the dialog
        // and likely reload the page
      } finally {
        setIsSubmitting(false);
      }
    };

    const handleKeyDown = (event: React.KeyboardEvent) => {
      if (event.key === "Enter" && !isSubmitting) {
        handleSubmit();
      }
    };

    const toggleShowToken = () => {
      setShowToken(!showToken);
    };

    // Different messages based on whether this is initial auth or re-auth
    const title = "Token Authentication Required";
    const message = hasExistingToken
      ? "The authentication token is invalid or has expired. Please provide a valid authentication token."
      : "Token authentication is enabled for this cluster. Please provide a valid authentication token.";

    return (
      <Dialog
        open={open}
        // Make dialog non-dismissible - user must provide valid token
        disableEscapeKeyDown
        aria-labelledby="token-auth-dialog-title"
        aria-describedby="token-auth-dialog-description"
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle id="token-auth-dialog-title">{title}</DialogTitle>
        <DialogContent>
          <DialogContentText
            id="token-auth-dialog-description"
            sx={{ marginBottom: 2 }}
          >
            {message}
          </DialogContentText>

          {error && (
            <Alert severity="error" sx={{ marginBottom: 2 }}>
              {error}
            </Alert>
          )}

          <TextField
            autoFocus
            fullWidth
            label="Authentication Token"
            type={showToken ? "text" : "password"}
            value={token}
            onChange={(e) => setToken(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={isSubmitting}
            placeholder="Enter your authentication token"
            InputProps={{
              endAdornment: (
                <InputAdornment position="end">
                  <IconButton
                    aria-label="toggle token visibility"
                    onClick={toggleShowToken}
                    edge="end"
                    disabled={isSubmitting}
                  >
                    {showToken ? <VisibilityOff /> : <Visibility />}
                  </IconButton>
                </InputAdornment>
              ),
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button
            onClick={handleSubmit}
            variant="contained"
            disabled={!token.trim() || isSubmitting}
            startIcon={isSubmitting ? <CircularProgress size={20} /> : null}
          >
            {isSubmitting ? "Validating..." : "Submit"}
          </Button>
        </DialogActions>
      </Dialog>
    );
  };

export default TokenAuthenticationDialog;
