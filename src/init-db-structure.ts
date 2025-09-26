import {DestPropType, ViewDefinition} from "./types";

export function traverseBFS(obj: Record<string, object>): string[] {
  const paths: string[] = [];
  const queue: { node: Record<string, object>; path: string}[] = [];

  // Enqueue the root object with an empty path
  queue.push({node: obj, path: ""});

  while (queue.length > 0) {
    // Dequeue the next node
    const item = queue.shift();
    if (!item) continue;

    const node = item.node;
    const path = item.path;

    // Enqueue the child objects with their paths
    for (const key in node) {
      if (typeof node[key] === "object" && node[key] !== null) {
        if (Array.isArray(node[key])) {
          for (const element of node[key] as string[]) {
            if (element.startsWith("ViewMap@") || element.startsWith("ViewArrayMap@") ) {
              const propView = `#${key}=${element}`;
              const viewPath = `${path}${propView}`;
              paths.push(viewPath);
              continue;
            }
            if (element.startsWith("View@")) {
              const viewPath = `${path}/${key}=${element}`;
              paths.push(viewPath);
              continue;
            }
          }
        } else {
          const newPath = path === "" ? key : `${path}/${key}`;
          paths.push(newPath);
          queue.push({node: node[key] as Record<string, object>, path: newPath});
        }
      }
    }
  }

  return paths;
}

export function mapDocPaths(paths: string[], Entity: Record<string, string>): Record<string, string> {
  const docPathsMap: Record<string, string> = {} as Record<string, string>;

  for (const entityKey of Object.values(Entity)) {
    const entityWithModifier = new RegExp(`/${entityKey}([#=][^/]*)?$`);
    const entityPaths = paths.filter((path) => entityWithModifier.test(path));
    if (entityPaths.length > 0) {
      const entityPath = entityPaths[0];
      docPathsMap[entityKey] = entityPath.split("/").map((element) => {
        element = element.split(/[#=]/)[0];
        if (Object.values(Entity).includes(element)) {
          return `{${element}Id}`;
        } else {
          return element;
        }
      }).join("/");
    }
  }

  return docPathsMap;
}

export function mapColPaths(docPathsMap: { [key: string]: string }): { [key: string]: string } {
  const colPathsMap: { [key: string]: string } = {};

  for (const [entityKey, docPath] of Object.entries(docPathsMap)) {
    colPathsMap[entityKey] = docPath.split("/").slice(0, -1).join("/");
  }

  return colPathsMap;
}

export function mapViewDefinitions(
  paths: string[],
  Entity: Record<string, string>,
): ViewDefinition[] {
  const viewDefs: ViewDefinition[] = [];

  for (const path of paths) {
    const match = path.match(/^([^#]*)(#.+)?=(View[^@]*@[^:]*:.+)$/);

    if (match) {
      const destPath = match[1];
      // Get the last word of the path
      const destEntity = destPath.split("/").slice(-1)[0];
      const destProp = match[2]?.substring(1);
      const viewDefinitionStr = match[3];

      const [typeVersion, srcEntity, srcPropsStr, optionsStr] = viewDefinitionStr.split(":");
      const srcProps = srcPropsStr.split(",");

      const [type, version] = typeVersion.split("@");
      let destType = "map";
      if (type === "ViewArrayMap") {
        destType = "array-map";
      }

      const options = optionsStr?.split(",")
        .reduce<Record<string, any>>((acc, pair) => {
          if (pair === "") return acc;
          const [key, rawValue] = pair.split("=");

          switch (key) {
          case "syncCreate": {
            if (rawValue === "true") {
              acc.syncCreate = true;
            } else if (rawValue === "false") {
              acc.syncCreate = false;
            } else {
              console.error(`SyncCreate option must be a boolean, got "${rawValue}"`);
            }
            return acc;
          }

          // Add other case for future options here

          default: {
            console.error(`Unsupported view option: ${key}`);
            return acc;
          }
          }
        }, {});


      // if srcEntity in Entity and destEntity exists
      if (Object.values(Entity).includes(srcEntity) && destEntity) {
        viewDefs.push({
          destEntity,
          srcProps,
          srcEntity,
          ...( destProp ? {
            destProp: {
              name: destProp,
              type: destType as DestPropType,
            },
          } : {}),
          ...(options && Object.values(options).length > 0 ? {options}: {}),
          version,
        });
      }
    }
  }

  return viewDefs;
}


export function initDbStructure(
  dbStructure: Record<string, object>,
  Entity: Record<string, string>
) {
  const paths = traverseBFS(dbStructure);
  const docPaths = mapDocPaths(paths, Entity);
  const viewDefinitions = mapViewDefinitions(paths, Entity);
  const docPathsRegex: Record<string, RegExp> = {} as Record<string, RegExp>;
  for (const [key, value] of Object.entries(docPaths)) {
    const regexPattern = value.replace(/{([^/]+)Id}/g, "([^/]+)");
    docPathsRegex[key] = new RegExp(`^${regexPattern}$`);
  }
  const colPaths = mapColPaths(docPaths);
  return {docPaths, docPathsRegex, colPaths, viewDefinitions};
}
