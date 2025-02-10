import { useEffect, useState } from "react";
import { getVirtualClusters } from "../../../service/virtual_cluster";
import { VirtualCluster } from "../../../type/virtual_cluster";

export const useVirtualClusters = () => {
  const [clusters, setClusters] = useState<VirtualCluster[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const fetchClusters = async () => {
      try {
        const data = await getVirtualClusters();
        setClusters(data);
      } catch (err) {
        setError(err instanceof Error ? err : new Error("Unknown error"));
      } finally {
        setIsLoading(false);
      }
    };

    fetchClusters();

    const interval = setInterval(fetchClusters, 4000);
    return () => clearInterval(interval);
  }, []);

  return { clusters, isLoading, error };
};
