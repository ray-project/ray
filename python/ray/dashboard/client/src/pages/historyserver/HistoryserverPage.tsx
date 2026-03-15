import {
  Alert,
  Box,
  Button,
  Card,
  CircularProgress,
  Grid,
  Typography,
} from "@mui/material";
import React, { useEffect, useState } from "react";

export type Cluster = {
  name: string;
  namespace: string;
  sessionName: string;
  createTime: string;
};

export const HistoryserverPage = () => {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [entering, setEntering] = useState<string | null>(null);

  useEffect(() => {
    const fetchClusters = async () => {
      try {
        const response = await fetch("/clusters");
        if (!response.ok) {
          throw new Error("Failed to fetch clusters");
        }
        const data = await response.json();
        setClusters(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    };
    fetchClusters();
  }, []);

  const handleEnterCluster = async (cluster: Cluster) => {
    setEntering(`${cluster.name}_${cluster.namespace}`);
    setError(null);
    try {
      const response = await fetch(
        `/enter_cluster/${cluster.namespace}/${cluster.name}/${cluster.sessionName}`,
      );
      if (response.ok) {
        // Redirect to the main dashboard now that the cookie is set
        window.location.href = "/#/overview";
      } else {
        const errorText = await response.text();
        throw new Error(
          `Failed to enter cluster. Server responded with: ${
            errorText || response.statusText
          }`,
        );
      }
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "An unknown error occurred while connecting to the server.",
      );
    } finally {
      setEntering(null);
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" p={10}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box p={4}>
      <Typography variant="h4" gutterBottom>
        KubeRay History Server
      </Typography>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      <Alert severity="info" sx={{ mb: 4 }}>
        Select a cluster to view its historical dashboard.
      </Alert>
      <Grid container spacing={3}>
        {clusters.map((cluster) => {
          const clusterId = `${cluster.name}_${cluster.namespace}`;
          return (
            <Grid item xs={12} sm={6} md={4} key={clusterId}>
              <Card
                variant="outlined"
                sx={{
                  p: 3,
                  height: "100%",
                  display: "flex",
                  flexDirection: "column",
                }}
              >
                <Typography variant="h6" color="primary" gutterBottom>
                  {cluster.name}
                </Typography>
                <Typography variant="body2" color="textSecondary">
                  <strong>Namespace:</strong> {cluster.namespace}
                </Typography>
                <Typography
                  variant="body2"
                  color="textSecondary"
                  sx={{ mb: 2 }}
                >
                  <strong>Created:</strong> {cluster.createTime}
                </Typography>
                <Box sx={{ mt: "auto" }}>
                  <Button
                    fullWidth
                    variant="contained"
                    color="primary"
                    disabled={entering !== null}
                    onClick={() => handleEnterCluster(cluster)}
                  >
                    {entering === clusterId ? "Entering..." : "Enter Cluster"}
                  </Button>
                </Box>
              </Card>
            </Grid>
          );
        })}
      </Grid>
    </Box>
  );
};
