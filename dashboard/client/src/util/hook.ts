import { get } from "lodash";
import { useState } from "react";

export const useFilter = <KeyType extends string>() => {
  const [filters, setFilters] = useState<{ key: KeyType; val: string }[]>([]);
  const changeFilter = (key: KeyType, val: string) => {
    const f = filters.find((e) => e.key === key);
    if (f) {
      f.val = val;
    } else {
      filters.push({ key, val });
    }
    setFilters([...filters]);
  };
  const filterFunc = (instance: { [key: string]: any }) => {
    return filters.every(
      (f) => !f.val || get(instance, f.key, "").toString().includes(f.val),
    );
  };

  return {
    changeFilter,
    filterFunc,
  };
};

export const useSorter = (initialSortKey?: string) => {
  const [sorter, setSorter] = useState({
    key: initialSortKey || "",
    desc: false,
  });

  const sorterFunc = (
    instanceA: { [key: string]: any },
    instanceB: { [key: string]: any },
  ) => {
    if (!sorter.key) {
      return 0;
    }

    let [b, a] = [instanceA, instanceB];
    if (sorter.desc) {
      [a, b] = [instanceA, instanceB];
    }

    if (!get(a, sorter.key)) {
      return -1;
    }

    if (!get(b, sorter.key)) {
      return 1;
    }

    return get(a, sorter.key) > get(b, sorter.key) ? 1 : -1;
  };

  return {
    sorterFunc,
    setSortKey: (key: string) => setSorter({ ...sorter, key }),
    setOrderDesc: (desc: boolean) => setSorter({ ...sorter, desc }),
    sorterKey: sorter.key,
  };
};
