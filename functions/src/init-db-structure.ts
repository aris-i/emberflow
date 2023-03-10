import {dbStructure, Entity} from "./custom/db-structure";


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


const paths = traverseBFS(dbStructure);
// console.log(paths);

function mapDocPaths(paths: string[]): Record<Entity, string> {
  const docPathsMap: Record<Entity, string> = {} as Record<Entity, string>;

  for (const entityKey of Object.values(Entity)) {
    const entityPaths = paths.filter((path) => path.endsWith(`/${entityKey}`));
    if (entityPaths.length > 0) {
      const entityPath = entityPaths[0];
      docPathsMap[entityKey] = entityPath.split("/").map((element) => {
        if (Object.values(Entity).includes(element as Entity)) {
          return `{${element}Id}`;
        } else {
          return element;
        }
      }).join("/");
    }
  }

  return docPathsMap;
}

const docPaths = mapDocPaths(paths);
// console.log(docPaths);

// Convert docPaths values into regex patterns
const docPathsRegex: Record<Entity, RegExp> = {} as Record<Entity, RegExp>;
for (const [key, value] of Object.entries(docPaths)) {
  const regexPattern = value.replace(/{(\w+)Id}/g, "([^/]+)");
  docPathsRegex[key as Entity] = new RegExp(`^${regexPattern}$`);
}

// const entityRegex = findMatchingDocPathRegex("users/3234/organizations/231");
// console.log(entityRegex);

function mapColPaths(docPathsMap: { [key: string]: string }): { [key: string]: string } {
  const colPathsMap: { [key: string]: string } = {};

  for (const [entityKey, docPath] of Object.entries(docPathsMap)) {
    colPathsMap[entityKey] = docPath.split("/").slice(0, -1).join("/");
  }

  return colPathsMap;
}

const colPaths = mapColPaths(docPaths);
// console.log(colPaths);

// const subPaths1 = filterSubDocPathsByEntity(Entity.Project);
// console.log(subPaths1);

// const groupedPaths = expandAndGroupDocPaths("users/12343/organizations/3214/projects/2314");
// console.log("group paths", groupedPaths);

export {docPaths, docPathsRegex, colPaths};
