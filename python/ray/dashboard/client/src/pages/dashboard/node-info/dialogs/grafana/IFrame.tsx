import { fade } from "@material-ui/core/styles/colorManipulator";
import { Theme } from "@material-ui/core/styles/createMuiTheme";
import createStyles from "@material-ui/core/styles/createStyles";
import withStyles, { WithStyles } from "@material-ui/core/styles/withStyles";
import Typography from "@material-ui/core/Typography";
import React from "react";
import { getGrafanaIframe, GetGrafanaIframeResponse } from "../../../../../api";
import DialogWithTitle from "../../../../../common/DialogWithTitle";

const styles = (theme: Theme) =>
  createStyles({
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3)
    }
  });

interface Props {
  clearDialog: () => void;
  pid: number | "All";
  metric: "cpu" | "memory";
}

interface State {
  result: GetGrafanaIframeResponse | null;
  error: string | null;
}

class IFrame extends React.Component<Props & WithStyles<typeof styles>, State> {
  state: State = {
    result: null,
    error: null
  };

  async componentDidMount() {
    try {
      const { metric, pid } = this.props;
      const result = await getGrafanaIframe(pid, metric);
      this.setState({ result, error: null });
    } catch (error) {
      this.setState({ result: null, error: error.toString() });
    }
  }

  render() {
    const { classes, clearDialog } = this.props;
    const { result, error } = this.state;

    return (
      <DialogWithTitle handleClose={clearDialog} title="Historical Metrics">
        {error !== null ? (
          <Typography color="error">{error}</Typography>
        ) : result === null ? (
          <Typography color="textSecondary">Loading...</Typography>
        ) : (
          <div dangerouslySetInnerHTML={{ __html: result.frame_html }} />
        )}
      </DialogWithTitle>
    );
  }
}

export default withStyles(styles)(IFrame);
