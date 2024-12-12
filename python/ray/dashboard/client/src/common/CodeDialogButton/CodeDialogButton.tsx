import { Box, Card, Link, SxProps, Theme, Typography } from "@mui/material";
import yaml from "js-yaml";
import React, { useState } from "react";
import DialogWithTitle from "../DialogWithTitle";
import { ClassNameProps } from "../props";

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
  sx?: SxProps<Theme>;
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
      <Link
        sx={{
          whiteSpace: "nowrap",
        }}
        component="button"
        onClick={handleConfigClick}
      >
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
            <Typography
              sx={{
                whiteSpace: "pre",
                fontFamily:
                  "SFMono-Regular,Consolas,Liberation Mono,Menlo,monospace",
                padding: 2,
                overflow: "scroll",
                maxHeight: 600,
              }}
            >
              {typeof code === "string" ? code : yaml.dump(code, { indent: 2 })}
            </Typography>
          </Card>
        </DialogWithTitle>
      )}
    </React.Fragment>
  );
};

type CodeDialogButtonWithPreviewProps = CodeDialogButtonProps & ClassNameProps;
/**
 * Similar to CodeDialogButton but also shows a snippet of the expanded text next to the button.
 */
export const CodeDialogButtonWithPreview = ({
  code,
  buttonText,
  className,
  sx,
  ...props
}: CodeDialogButtonWithPreviewProps) => {
  const codeText =
    typeof code === "string"
      ? code
      : yaml.dump(code, { indent: 2, sortKeys: true });

  const buttonTextToPass = buttonText ?? "Expand";

  return (
    <Box
      className={className}
      sx={[
        {
          display: "flex",
          flexWrap: "nowrap",
          flexDirection: "row",
          gap: 1,
        },
        ...(Array.isArray(sx) ? sx : [sx]),
      ]}
    >
      <Box
        component="span"
        sx={{
          display: "block",
          whiteSpace: "nowrap",
          overflow: "hidden",
          textOverflow: "ellipsis",
          flex: 1,
        }}
      >
        {codeText}
      </Box>
      <CodeDialogButton
        code={codeText}
        buttonText={buttonTextToPass}
        {...props}
      />
    </Box>
  );
};
