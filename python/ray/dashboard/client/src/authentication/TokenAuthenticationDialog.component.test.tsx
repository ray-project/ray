import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import "@testing-library/jest-dom";
import TokenAuthenticationDialog from "./TokenAuthenticationDialog";

describe("TokenAuthenticationDialog", () => {
  const mockOnSubmit = jest.fn();

  beforeEach(() => {
    mockOnSubmit.mockClear();
  });

  it("renders with initial message when no existing token", () => {
    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    expect(
      screen.getByText("Token Authentication Required"),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/token authentication is enabled for this cluster/i),
    ).toBeInTheDocument();
  });

  it("renders with re-authentication message when has existing token", () => {
    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={true}
        onSubmit={mockOnSubmit}
      />,
    );

    expect(
      screen.getByText("Token Authentication Required"),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/authentication token is invalid or has expired/i),
    ).toBeInTheDocument();
  });

  it("displays error message when provided", () => {
    const errorMessage = "Invalid token provided";
    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
        error={errorMessage}
      />,
    );

    expect(screen.getByText(errorMessage)).toBeInTheDocument();
  });

  it("calls onSubmit with entered token when submit is clicked", async () => {
    const user = userEvent.setup();
    mockOnSubmit.mockResolvedValue(undefined);

    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    const input = screen.getByLabelText(/authentication token/i);
    await user.type(input, "test-token-123");

    const submitButton = screen.getByRole("button", { name: /submit/i });
    await user.click(submitButton);

    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith("test-token-123");
    });
  });

  it("calls onSubmit when Enter key is pressed", async () => {
    const user = userEvent.setup();
    mockOnSubmit.mockResolvedValue(undefined);

    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    const input = screen.getByLabelText(/authentication token/i);
    await user.type(input, "test-token-123{Enter}");

    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith("test-token-123");
    });
  });

  it("disables submit button when token is empty", () => {
    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    const submitButton = screen.getByRole("button", { name: /submit/i });
    expect(submitButton).toBeDisabled();
  });

  it("enables submit button when token is entered", async () => {
    const user = userEvent.setup();
    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    const submitButton = screen.getByRole("button", { name: /submit/i });
    expect(submitButton).toBeDisabled();

    const input = screen.getByLabelText(/authentication token/i);
    await user.type(input, "test-token");

    expect(submitButton).not.toBeDisabled();
  });

  it("toggles token visibility when visibility icon is clicked", async () => {
    const user = userEvent.setup();
    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    const input = screen.getByLabelText(/authentication token/i);
    await user.type(input, "secret-token");

    // Initially should be password type (hidden)
    expect(input).toHaveAttribute("type", "password");

    // Click visibility toggle
    const toggleButton = screen.getByLabelText(/toggle token visibility/i);
    await user.click(toggleButton);

    // Should now be text type (visible)
    expect(input).toHaveAttribute("type", "text");

    // Click again to hide
    await user.click(toggleButton);
    expect(input).toHaveAttribute("type", "password");
  });

  it("shows loading state during submission", async () => {
    const user = userEvent.setup();
    // Mock a slow submission
    mockOnSubmit.mockImplementation(
      () => new Promise((resolve) => setTimeout(resolve, 100)),
    );

    render(
      <TokenAuthenticationDialog
        open={true}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    const input = screen.getByLabelText(/authentication token/i);
    await user.type(input, "test-token");

    const submitButton = screen.getByRole("button", { name: /submit/i });
    await user.click(submitButton);

    // Should show validating state
    await waitFor(() => {
      expect(screen.getByText(/validating.../i)).toBeInTheDocument();
    });
  });

  it("does not render when open is false", () => {
    render(
      <TokenAuthenticationDialog
        open={false}
        hasExistingToken={false}
        onSubmit={mockOnSubmit}
      />,
    );

    // Dialog should not be visible
    expect(
      screen.queryByText("Token Authentication Required"),
    ).not.toBeInTheDocument();
  });
});
