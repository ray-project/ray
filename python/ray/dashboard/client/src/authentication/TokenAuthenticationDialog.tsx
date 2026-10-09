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

    // Different messages based on whether this is initial auth or re-auth.
    const title = "Authentication Token Required";
    const message =
      (hasExistingToken
        ? "The existing authentication token is invalid."
        : "Token authentication is enabled.") +
      " Provide the matching authentication token for this cluster.\n- Local clusters: use `ray get-auth-token` to retrieve it.\n- Remote clusters: you must retrieve the token that was used when creating the cluster.\n\nSee: https://docs.ray.io/en/latest/ray-security/token-auth.html";

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
            sx={{ marginBottom: 2, whiteSpace: "pre-line" }}
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
                    {showToken ? (
                      <VisibilityOff
                        sx={(theme) => ({
                          color: theme.palette.text.secondary,
                        })}
                      />
                    ) : (
                      <Visibility
                        sx={(theme) => ({
                          color: theme.palette.text.secondary,
                        })}
                      />
                    )}
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
