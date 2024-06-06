import { Card, Link, Typography } from "@mui/material";
import { styled } from "@mui/material/styles"
import yaml from "js-yaml";
import React, { useState } from "react";
import DialogWithTitle from "../DialogWithTitle";
import { ClassNameProps } from "../props";

const ConfigTextTypography = styled(Typography)(({theme}) => ({
  whiteSpace: "pre",
  fontFamily: "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
  padding: theme.spacing(2),
  overflow: "scroll",
  maxHeight: 600,
}));

export type CodeDialogButtonProps = {
  /**
   * Title of the dialog box
   */
  title: string;
  /**
   * Text for the button. By default it's "View"
   */
  buttonText?: string;
  /**
   * Code to show in the dialog. If an object is passed in, that object will be stringified to yaml.
   */
  code: string | object;
};

/**
 * A button that when clicked, will pop up a dialog with the full code text with proper formatting.
 */
export const CodeDialogButton = ({
  title,
  buttonText = "View",
  code,
}: CodeDialogButtonProps) => {

  const [showConfigDialog, setShowConfigDialog] = useState(false);

  const handleConfigClick = () => {
    setShowConfigDialog(true);
  };

  return (
    <React.Fragment>
      <Link component="button" onClick={handleConfigClick}>
        {buttonText}
      </Link>
      {showConfigDialog && (
        <DialogWithTitle
          title={title}
          handleClose={() => {
            setShowConfigDialog(false);
          }}
        >
          <Card variant="outlined">
            <ConfigTextTypography>
              {typeof code === "string" ? code : yaml.dump(code, { indent: 2 })}
            </ConfigTextTypography>
          </Card>
        </DialogWithTitle>
      )}
    </React.Fragment>
  );
};

const RootDiv = styled("div")(({theme}) => ({
  display: "flex",
  flexWrap: "nowrap",
  flexDirection: "row",
  gap: theme.spacing(1),
}));

const PreviewTextSpan = styled("span")(({theme}) => ({
  display: "block",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
  flex: 1,
}));

type CodeDialogButtonWithPreviewProps = CodeDialogButtonProps & ClassNameProps;
/**
 * Similar to CodeDialogButton but also shows a snippet of the expanded text next to the button.
 */
export const CodeDialogButtonWithPreview = ({
  code,
  buttonText,
  className,
  ...props
}: CodeDialogButtonWithPreviewProps) => {
  const codeText =
    typeof code === "string"
      ? code
      : yaml.dump(code, { indent: 2, sortKeys: true });

  const buttonTextToPass = buttonText ?? "Expand";

  return (
    <RootDiv className={className}>
      <PreviewTextSpan>{codeText}</PreviewTextSpan>
      <CodeDialogButton
        code={codeText}
        buttonText={buttonTextToPass}
        {...props}
      />
    </RootDiv>
  );
};
