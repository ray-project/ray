import axios from "axios";

type CMDRsp = {
  result: boolean;
  msg: string;
  data: {
    output: string;
  };
};

export const getJstack = (ip: string, pid: string) => {
  return axios.get<CMDRsp>("utils/jstack", {
    params: {
      ip,
      pid,
    },
  });
};

export const getJmap = (ip: string, pid: string) => {
  return axios.get<CMDRsp>("utils/jmap", {
    params: {
      ip,
      pid,
    },
  });
};

export const getJstat = (ip: string, pid: string, options: string) => {
  return axios.get<CMDRsp>("utils/jstat", {
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
  return axios.get<NamespacesRsp>("namespaces");
};
