import { Box, IconButton, Link, Tooltip, Typography } from "@mui/material";
import copy from "copy-to-clipboard";
import React, { useState } from "react";
import { RiFileCopyLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";
import { Section } from "../../common/Section";
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

type CopyAndLinkableMetadataContent = LinkableMetadataContent &
  CopyableMetadataContent;

export type Metadata = {
  readonly label: string;
  readonly labelTooltip?: string | JSX.Element;

  // If content is undefined, we display "-" as the placeholder.
  readonly content?:
    | StringOnlyMetadataContent
    | LinkableMetadataContent
    | CopyableMetadataContent
    | CopyAndLinkableMetadataContent
    | JSX.Element;

  /**
   * This flag will determine this metadata field will show in the UI.
   * Defaults to true.
   */
  readonly isAvailable?: boolean;
};

const styles = {
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
};

/**
 * We style the metadata content based on the type supplied.
 *
 * A default style will be applied if content is MetadataContent type.
 * If content is undefined, we display "-" as the placeholder.
 */
export const MetadataContentField: React.FC<{
  content: Metadata["content"];
  label: string;
}> = ({ content, label }) => {
  const [copyIconClicked, setCopyIconClicked] = useState<boolean>(false);

  const copyElement = content && "copyableValue" in content && (
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
        onMouseLeave={() => setTimeout(() => setCopyIconClicked(false), 333)}
        size="small"
        sx={{ color: "black", marginLeft: 0.5 }}
      >
        <RiFileCopyLine />
      </IconButton>
    </Tooltip>
  );

  if (content === undefined || "value" in content) {
    return content === undefined ||
      !("link" in content) ||
      content.link === undefined ? (
      <Box sx={styles.contentContainer}>
        <Typography
          sx={styles.content}
          variant="body2"
          title={content?.value}
          data-testid={`metadata-content-for-${label}`}
        >
          {content?.value ?? "-"}
        </Typography>
        {copyElement}
      </Box>
    ) : content.link.startsWith("http") ? (
      <Box sx={styles.contentContainer}>
        <Link
          sx={styles.content}
          href={content.link}
          data-testid={`metadata-content-for-${label}`}
        >
          {content.value}
        </Link>
        {copyElement}
      </Box>
    ) : (
      <Box sx={styles.contentContainer}>
        <Link
          sx={styles.content}
          component={RouterLink}
          to={content.link}
          data-testid={`metadata-content-for-${label}`}
        >
          {content.value}
        </Link>
        {copyElement}
      </Box>
    );
  }
  return <div data-testid={`metadata-content-for-${label}`}>{content}</div>;
};

/**
 * Renders the metadata list in a column format.
 */
const MetadataList: React.FC<{
  metadataList: Metadata[];
}> = ({ metadataList }) => {
  const filteredMetadataList = metadataList.filter(
    ({ isAvailable }) => isAvailable ?? true,
  );
  return (
    <Box
      sx={{
        display: "grid",
        gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
        rowGap: 1,
        columnGap: 4,
      }}
    >
      {filteredMetadataList.map(({ label, labelTooltip, content }, idx) => (
        <Box key={idx} flex={1} paddingTop={0.5} paddingBottom={0.5}>
          <Box display="flex" alignItems="center" marginBottom={0.5}>
            <Typography
              sx={{ color: (theme) => theme.palette.text.secondary }}
              variant="body2"
            >
              {label}
            </Typography>
            {labelTooltip && (
              <HelpInfo sx={{ marginLeft: 0.5 }}>{labelTooltip}</HelpInfo>
            )}
          </Box>
          <MetadataContentField content={content} label={label} />
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
  footer,
}: {
  header?: string;
  metadataList: Metadata[];
  footer?: JSX.Element;
}) => {
  return (
    <Section title={header} marginTop={1} marginBottom={4}>
      <MetadataList metadataList={metadataList} />
      <Box marginTop={1}>{footer}</Box>
    </Section>
  );
};
