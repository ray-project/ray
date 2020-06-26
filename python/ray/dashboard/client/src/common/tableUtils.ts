export const descendingComparator = <T>(a: T, b: T, orderBy: keyof T) => {
  if (b[orderBy] < a[orderBy]) {
    return -1;
  }
  if (b[orderBy] > a[orderBy]) {
    return 1;
  }
  return 0;
};

const descendingComparatorFnAccessor = <T>(
  a: T,
  b: T,
  orderByFn: Accessor<T>,
) => {
  const aVal = orderByFn(a);
  const bVal = orderByFn(b);
  if (bVal < aVal) {
    return -1;
  }
  if (bVal > aVal) {
    return 1;
  }
  return 0;
};

export type Order = "asc" | "desc";
export type Comparator<T> = (a: T, b: T) => number;
export type Accessor<T> = (a: T) => number | string;

export const getComparator = <Key extends keyof any>(
  order: Order,
  orderBy: Key,
): ((
  a: { [key in Key]: number | string },
  b: { [key in Key]: number | string },
) => number) => {
  return order === "desc"
    ? (a, b) => descendingComparator(a, b, orderBy)
    : (a, b) => -descendingComparator(a, b, orderBy);
};

export const getFnComparator = <T>(order: Order, orderByFn: Accessor<T>) => (
  a: T,
  b: T,
): number => {
  return order === "desc"
    ? descendingComparatorFnAccessor(a, b, orderByFn)
    : -descendingComparatorFnAccessor(a, b, orderByFn);
};

export const stableSort = <T>(array: T[], comparator: Comparator<T>) => {
  const stabilizedThis = array.map((el, index) => [el, index] as [T, number]);
  stabilizedThis.sort((a, b) => {
    const order = comparator(a[0], b[0]);
    if (order !== 0) {
      return order;
    }
    return a[1] - b[1];
  });
  return stabilizedThis.map((el) => el[0]);
};
