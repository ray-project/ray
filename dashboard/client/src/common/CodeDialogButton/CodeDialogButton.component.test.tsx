import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { CodeDialogButton } from "./CodeDialogButton";

describe("CodeDialogButton", () => {
  it("renders", async () => {
    expect.assertions(3);

    render(
      <CodeDialogButton title="Test title" json={{ foo: 1, bar: "bar" }} />,
    );

    const user = userEvent.setup();

    await screen.findByText("View");
    expect(screen.getByText("View")).toBeVisible();
    user.click(screen.getByText("View"));

    await screen.findByText(/"foo": 1/);
    expect(screen.getByText(/"foo": 1/)).toBeVisible();
    expect(screen.getByText(/"bar": "bar"/)).toBeVisible();
  });

  it("renders with custom button text", async () => {
    expect.assertions(1);

    render(
      <CodeDialogButton
        title="Test title"
        buttonText="CustomButton"
        json={{ foo: 1, bar: "bar" }}
      />,
    );

    await screen.findByText("CustomButton");
    expect(screen.getByText("CustomButton")).toBeVisible();
  });
});
