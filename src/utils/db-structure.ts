import {DestPropType} from "../types";

function view(entity: string, props: string[], options?: Record<string, any>, version?: string): string {
  const optionStr = formOptionStr(options);

  return `View@${version || "0.0.0"}:${entity}:${props.join(",")}:${optionStr}`;
}

function propView(
  type: DestPropType, entity: string, props: string[], options?: Record<string, any>, version?: string
): string {
  const optionStr = formOptionStr(options);

  const viewType = type === "map" ? "ViewMap" : "ViewArrayMap";
  return `${viewType}@${version || "0.0.0"}:${entity}:${props.join(",")}:${optionStr}`;
}

function formOptionStr(options: Record<string, any> | undefined) {
  return Object.entries(options || {})
    .map(([key, value]) => `${key}=${value}`)
    .join(",");
}

export {view, propView};
