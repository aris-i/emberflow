
function traverseBFS(obj: Record<string, object>): string[] {
  const paths: string[] = [];
  const queue: { node: Record<string, object>; path: string }[] = [];

  // Enqueue the root object with an empty path
  queue.push({node: obj, path: ""});

  while (queue.length > 0) {
    // Dequeue the next node
    const item = queue.shift();
    if (!item) continue;

    const node = item.node;
    const path = item.path;

    // Only add non-blank paths to the paths array
    if (path !== "") {
      paths.push(path);
    }

    // Enqueue the child objects with their paths
    for (const key in node) {
      if (typeof node[key] === "object" && node[key] !== null) {
        const childPath = path === "" ? key : `${path}/${key}`;
        queue.push({node: node[key] as Record<string, object>, path: childPath});
      }
    }
  }

  return paths;
}

function mapDocPaths(paths: string[], Entity: Record<string, string>): Record<string, string> {
  const docPathsMap: Record<string, string> = {} as Record<string, string>;

  for (const entityKey of Object.values(Entity)) {
    const entityPaths = paths.filter((path) => path.endsWith(`/${entityKey}`));
    if (entityPaths.length > 0) {
      const entityPath = entityPaths[0];
      docPathsMap[entityKey] = entityPath.split("/").map((element) => {
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

function mapColPaths(docPathsMap: { [key: string]: string }): { [key: string]: string } {
  const colPathsMap: { [key: string]: string } = {};

  for (const [entityKey, docPath] of Object.entries(docPathsMap)) {
    colPathsMap[entityKey] = docPath.split("/").slice(0, -1).join("/");
  }

  return colPathsMap;
}

export function initDbStructure(dbStructure: Record<string, object>, Entity: Record<string, string>) {
  const paths = traverseBFS(dbStructure);
  const docPaths = mapDocPaths(paths, Entity);
  const docPathsRegex: Record<string, RegExp> = {} as Record<string, RegExp>;
  for (const [key, value] of Object.entries(docPaths)) {
    const regexPattern = value.replace(/{(\w+)Id}/g, "([^/]+)");
    docPathsRegex[key] = new RegExp(`^${regexPattern}$`);
  }
  const colPaths = mapColPaths(docPaths);
  return {docPaths, docPathsRegex, colPaths};
}
