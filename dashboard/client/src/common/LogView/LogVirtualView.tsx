import low from "lowlight";
import React, {
  CSSProperties,
  MutableRefObject,
  useEffect,
  useRef,
  useState,
} from "react";
import { FixedSizeList as List } from "react-window";
import "./darcula.css";
import "./github.css";
import "./index.css";

const uniqueKeySelector = () => Math.random().toString(16).slice(-8);

const value2react = (
  { type, tagName, properties, children, value = "" }: any,
  key: string,
  keywords: string = "",
) => {
  switch (type) {
    case "element":
      return React.createElement(
        tagName,
        {
          className: properties.className[0],
          key: `${key}line${uniqueKeySelector()}`,
        },
        children.map((e: any, i: number) =>
          value2react(e, `${key}-${i}`, keywords),
        ),
      );
    case "text":
      if (keywords && value.includes(keywords)) {
        const afterChildren = [];
        const vals = value.split(keywords);
        let tmp = vals.shift();
        if (!tmp) {
          return React.createElement(
            "span",
            { className: "find-kws" },
            keywords,
          );
        }
        while (typeof tmp === "string") {
          if (tmp !== "") {
            afterChildren.push(tmp);
          } else {
            afterChildren.push(
              React.createElement("span", { className: "find-kws" }, keywords),
            );
          }

          tmp = vals.shift();
          if (tmp) {
            afterChildren.push(
              React.createElement("span", { className: "find-kws" }, keywords),
            );
          }
        }
        return afterChildren;
      }
      return value;
    default:
      return [];
  }
};

export type LogVirtualViewProps = {
  content: string;
  width?: number;
  height?: number;
  fontSize?: number;
  theme?: "light" | "dark";
  language?: string;
  focusLine?: number;
  keywords?: string;
  style?: { [key: string]: string | number };
  listRef?: MutableRefObject<HTMLDivElement | null>;
  onScrollBottom?: (event: Event) => void;
  revert?: boolean;
};

type HastNode = lowlight.HastNode;

const LogVirtualView: React.FC<LogVirtualViewProps> = ({
  content,
  width = "100%",
  height,
  fontSize = 12,
  theme = 'light',
  keywords = "",
  language = "dos",
  focusLine = 1,
  style = {},
  listRef,
  onScrollBottom,
  revert = false,
}) => {
  const [logs, setLogs] = useState<
    { value: HastNode[]; i: number; origin: string }[]
  >([]);
  const total = logs.length;
  const timmer = useRef<ReturnType<typeof setTimeout>>();
  const el = useRef<List>(null);
  const outter = useRef<HTMLDivElement>(null);
  if (listRef) {
    listRef.current = outter.current;
  }
  const itemRenderer = ({
    index,
    style: s,
  }: {
    index: number;
    style: CSSProperties;
  }) => {
    const { value, i } = logs[revert ? logs.length - 1 - index : index];
    return (
      <div
        key={`${index}list`}
        style={{ ...s, overflowX: "visible", whiteSpace: "pre" }}
      >
        <span
          style={{
            marginRight: 4,
            width: 40,
            color: "#999",
            display: "inline-block",
          }}
        >
          {i + 1}
        </span>
        {value.map((v) => value2react(v, index.toString(), keywords))}
      </div>
    );
  };

  useEffect(() => {
    const originContent = content.split("\n");
    if (timmer.current) {
      clearTimeout(timmer.current);
    }
    timmer.current = setTimeout(() => {
      setLogs(
        originContent
          .map((e, i) => ({ i, origin: e }))
          .filter((e) => e.origin.includes(keywords))
          .map((e) => ({
            ...e,
            value: low.highlight(language, e.origin).value,
          })),
      );
    }, 300);
  }, [content, keywords, language]);

  useEffect(() => {
    if (el.current) {
      el.current?.scrollTo((focusLine - 1) * (fontSize + 6));
    }
  }, [focusLine, fontSize]);

  useEffect(() => {
    if (outter.current) {
      const scrollFunc = (event: any) => {
        const { target } = event;
        if (
          target &&
          target.scrollTop + target.clientHeight === target.scrollHeight
        ) {
          if (onScrollBottom) {
            onScrollBottom(event);
          }
        }
      };
      outter.current.addEventListener("scroll", scrollFunc);
      return () => outter?.current?.removeEventListener("scroll", scrollFunc);
    }
  }, [onScrollBottom]);

  return (
    <List
      height={height || (content.split("\n").length + 1) * 18}
      width={width}
      ref={el}
      outerRef={outter}
      className={`hljs-${theme}`}
      style={{
        fontSize,
        ...style,
      }}
      itemSize={fontSize + 6}
      itemCount={total}
    >
      {itemRenderer}
    </List>
  );
};

export default LogVirtualView;
