import { axiosInstance } from "./requestHandlers";

type CMDRsp = {
  result: boolean;
  msg: string;
  data: {
    output: string;
  };
};

export const getJstack = (ip: string, pid: string) => {
  return axiosInstance.get<CMDRsp>("utils/jstack", {
    params: {
      ip,
      pid,
    },
  });
};

export const getJmap = (ip: string, pid: string) => {
  return axiosInstance.get<CMDRsp>("utils/jmap", {
    params: {
      ip,
      pid,
    },
  });
};

export const getJstat = (ip: string, pid: string, options: string) => {
  return axiosInstance.get<CMDRsp>("utils/jstat", {
    params: {
      ip,
      pid,
      options,
    },
  });
};

type NamespacesRsp = {
  result: boolean;
  msg: string;
  data: {
    namespaces: {
      namespaceId: string;
      hostNameList: string[];
    }[];
  };
};

export const getNamespaces = () => {
  return axiosInstance.get<NamespacesRsp>("namespaces");
};

type TorchTraceRsp = {
  success: boolean;
  output: string;
};

export const getTorchTrace = (ip: string, pid: string, numIterations = 4) => {
  return axiosInstance.get<TorchTraceRsp>("worker/gpu_profile", {
    params: {
      ip,
      pid,
      num_iterations: numIterations,
    },
    // Allow following redirects and handle as blob for file download
    maxRedirects: 5,
  });
};
