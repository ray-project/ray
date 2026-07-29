import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "./CodeDialogButton";

describe("CodeDialogButton", () => {
  it("renders with code as JSON", async () => {
    expect.assertions(4);

    render(
      <CodeDialogButton title="Test title" code={{ foo: 1, bar: "bar" }} />,
      { wrapper: TEST_APP_WRAPPER },
    );

    const user = userEvent.setup();

    await screen.findByText("View");
    expect(screen.getByText("View")).toBeVisible();
    await user.click(screen.getByText("View"));

    await screen.findByText("Test title");
    expect(screen.getByText("Test title")).toBeVisible();
    expect(screen.getByText(/foo: 1/)).toBeVisible();
    expect(screen.getByText(/bar: bar/)).toBeVisible();
  });

  it("renders with custom button text and code as a string", async () => {
    expect.assertions(4);

    render(
      <CodeDialogButton
        title="Test title"
        buttonText="CustomButton"
        code="import ray\nray.init()"
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    const user = userEvent.setup();

    await screen.findByText("CustomButton");
    expect(screen.getByText("CustomButton")).toBeVisible();
    await user.click(screen.getByText("CustomButton"));

    await screen.findByText("Test title");
    expect(screen.getByText("Test title")).toBeVisible();
    expect(screen.getByText(/import ray/)).toBeVisible();
    expect(screen.getByText(/ray.init\(\)/)).toBeVisible();
  });
});

describe("CodeDialogButtonWithPreview", () => {
  it("renders", async () => {
    expect.assertions(5);

    render(
      <CodeDialogButtonWithPreview
        title="Test title"
        code={{ foo: 1, bar: "bar" }}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    const user = userEvent.setup();

    await screen.findByText("Expand");
    // Preview of the code should be visible
    expect(screen.getByText(/foo: 1/)).toBeVisible();
    expect(screen.getByText("Expand")).toBeVisible();
    await user.click(screen.getByText("Expand"));

    await screen.findByText("Test title");
    expect(screen.getByText("Test title")).toBeVisible();
    expect(screen.getAllByText(/foo: 1/)[1]).toBeVisible();
    expect(screen.getAllByText(/bar: bar/)[1]).toBeVisible();
  });
});
