import {
  Box,
  createStyles,
  IconButton,
  Link,
  makeStyles,
  Paper,
  Tooltip,
  Typography,
} from "@material-ui/core";
import FileCopyOutlined from "@material-ui/icons/FileCopyOutlined";
import copy from "copy-to-clipboard";
import React, { useState } from "react";
import { Link as RouterLink } from "react-router-dom";
import { HelpInfo } from "../Tooltip";

export type StringOnlyMetadataContent = {
  readonly value: string;
};

type LinkableMetadataContent = StringOnlyMetadataContent & {
  readonly link: string;
};

type CopyableMetadataContent = StringOnlyMetadataContent & {
  /**
   * The "copyable value" may be different from "value"
   * in case we want to render a more readable text.
   */
  readonly copyableValue: string;
};

export type Metadata = {
  readonly label: string;
  readonly labelTooltip?: string | JSX.Element;

  // If content is undefined, we display "-" as the placeholder.
  readonly content?:
    | StringOnlyMetadataContent
    | LinkableMetadataContent
    | CopyableMetadataContent
    | JSX.Element;

  /**
   * This flag will determine this metadata field will show in the UI.
   * Defaults to true.
   */
  readonly isAvailable?: boolean;
};

const useStyles = makeStyles((theme) =>
  createStyles({
    root: {
      display: "grid",
      gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
      rowGap: theme.spacing(1),
      columnGap: theme.spacing(4),
      padding: theme.spacing(2),
    },
    label: {
      color: theme.palette.text.secondary,
    },
    labelTooltip: {
      marginLeft: theme.spacing(0.5),
    },
    contentContainer: {
      display: "flex",
      alignItems: "center",
    },
    content: {
      display: "block",
      textOverflow: "ellipsis",
      overflow: "hidden",
      whiteSpace: "nowrap",
    },
    button: {
      color: "black",
      marginLeft: theme.spacing(0.5),
    },
  }),
);

/**
 * We style the metadata content based on the type supplied.
 *
 * A default style will be applied if content is MetadataContent type.
 * If content is undefined, we display "-" as the placeholder.
 */
export const MetadataContentField: React.FC<{
  content: Metadata["content"];
}> = ({ content }) => {
  const classes = useStyles();
  const [copyIconClicked, setCopyIconClicked] = useState<boolean>(false);

  if (content === undefined || "value" in content) {
    return content === undefined || !("link" in content) ? (
      <div className={classes.contentContainer}>
        <Typography
          className={classes.content}
          variant="body2"
          title={content?.value}
        >
          {content?.value ?? "-"}
        </Typography>
        {content && "copyableValue" in content && (
          <Tooltip
            placement="top"
            title={copyIconClicked ? "Copied" : "Click to copy"}
          >
            <IconButton
              aria-label="copy"
              onClick={() => {
                setCopyIconClicked(true);
                copy(content.copyableValue);
              }}
              // Set up mouse events to avoid text changing while tooltip is visible
              onMouseEnter={() => setCopyIconClicked(false)}
              onMouseLeave={() =>
                setTimeout(() => setCopyIconClicked(false), 333)
              }
              size="small"
              className={classes.button}
            >
              <FileCopyOutlined />
            </IconButton>
          </Tooltip>
        )}
      </div>
    ) : content.link.startsWith("http") ? (
      <Link className={classes.content} href={content.link}>
        {content.value}
      </Link>
    ) : (
      <Link
        className={classes.content}
        component={RouterLink}
        to={content.link}
      >
        {content.value}
      </Link>
    );
  }
  return content;
};

/**
 * Renders the metadata list in a column format.
 */
const MetadataList: React.FC<{
  metadataList: Metadata[];
}> = ({ metadataList }) => {
  const classes = useStyles();

  const filteredMetadataList = metadataList.filter(
    ({ isAvailable }) => isAvailable ?? true,
  );
  return (
    <Box className={classes.root}>
      {filteredMetadataList.map(({ label, labelTooltip, content }, idx) => (
        <Box key={idx} flex={1} paddingTop={0.5} paddingBottom={0.5}>
          <Box display="flex" alignItems="center" marginBottom={0.5}>
            <Typography className={classes.label} variant="body2">
              {label}
            </Typography>
            {labelTooltip && (
              <HelpInfo className={classes.labelTooltip}>
                {labelTooltip}
              </HelpInfo>
            )}
          </Box>
          <MetadataContentField content={content} />
        </Box>
      ))}
    </Box>
  );
};

/**
 * Renders the Metadata UI with the header and metadata in a 3-column format.
 */
export const MetadataSection = ({
  header,
  metadataList,
}: {
  header?: string;
  metadataList: Metadata[];
}) => {
  return (
    <Box marginTop={1} marginBottom={4}>
      {header && (
        <Box paddingBottom={2}>
          <Typography variant="h2">{header}</Typography>
        </Box>
      )}
      <Paper variant="outlined">
        <MetadataList metadataList={metadataList} />
      </Paper>
    </Box>
  );
};
