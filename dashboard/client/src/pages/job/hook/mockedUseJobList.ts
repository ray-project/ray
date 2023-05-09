const JOB_LIST = [
  { id: 1, name: "Data analysis", status: "PENDING", color: "orange" },
  { id: 2, name: "Model training", status: "RUNNING", color: "blue" },
  { id: 3, name: "Deployment", status: "SUCCEEDED", color: "green" },
  { id: 4, name: "Testing", status: "STOPPED", color: "grey" },
  { id: 5, name: "Data cleaning", status: "FAILED", color: "red" },
];

export const mockedUseJobList = () => {
  return JOB_LIST;
};
