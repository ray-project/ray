import { render, screen } from "@testing-library/react";
import React from "react";

import { MetadataContentField } from "./MetadataSection";

const CONTENT_VALUE = "test_string";
const LINK_VALUE = "https://docs.ray.com/";
const COPYABLE_VALUE = "Copyable value";
const COPY_BUTTON_LABEL = "copy";

describe("MetadataContentField", () => {
  it("renders the content string", () => {
    expect.assertions(3);

    render(<MetadataContentField content={{ value: CONTENT_VALUE }} />);

    expect(screen.getByText(CONTENT_VALUE)).toBeInTheDocument();
    expect(screen.getByText(CONTENT_VALUE)).not.toHaveAttribute("href");
    expect(screen.queryByLabelText(COPY_BUTTON_LABEL)).not.toBeInTheDocument();
  });

  it("renders the content string with label", () => {
    expect.assertions(3);

    render(
      <MetadataContentField
        content={{ value: CONTENT_VALUE, link: LINK_VALUE }}
      />,
    );

    expect(screen.getByText(CONTENT_VALUE)).toBeInTheDocument();
    expect(screen.getByText(CONTENT_VALUE)).toHaveAttribute("href", LINK_VALUE);
    expect(screen.queryByLabelText(COPY_BUTTON_LABEL)).not.toBeInTheDocument();
  });

  it("renders the content string with copyable value", () => {
    expect.assertions(3);
    render(
      <MetadataContentField
        content={{ value: CONTENT_VALUE, copyableValue: COPYABLE_VALUE }}
      />,
    );
    expect(screen.getByText(CONTENT_VALUE)).toBeInTheDocument();
    expect(screen.getByText(CONTENT_VALUE)).not.toHaveAttribute("href");
    expect(screen.getByLabelText(COPY_BUTTON_LABEL)).toBeInTheDocument();
  });

  it("renders the content string with a JSX element", () => {
    expect.assertions(2);

    const CUSTOM_TEST_ID = "custom-test-id";
    const customElement = <p data-testid={CUSTOM_TEST_ID}>Test</p>;
    render(<MetadataContentField content={customElement} />);

    expect(screen.queryByLabelText(COPY_BUTTON_LABEL)).not.toBeInTheDocument();
    expect(screen.getByTestId(CUSTOM_TEST_ID)).toBeInTheDocument();
  });
});
