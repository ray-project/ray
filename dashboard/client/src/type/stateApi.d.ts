import { Task } from "./task";

export type StateApiResponse<T> = {
  result: boolean;
  message: string;
  data: {
    [result: string]: {
      [result: string]: Array<T>;
    };
  };
};

export type AsyncFunction<O> = () => Promise<O>;
export type StateApiTypes = Task | PlacementGroup;
