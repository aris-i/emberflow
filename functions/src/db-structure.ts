enum Entity {
    User = "user",
    Organization = "organization",
    Project = "project",
    ProjectAccessList = "projectAccessList",
    Member = "member",
    Form = "form",
    Asset = "asset",
    AssetAccessList = "assetAccessList",
    Country = "country",
}

const dbStructure = {
  users: {
    [Entity.User]: {
      organizations: {
        [Entity.Organization]: {
          projects: {
            [Entity.Project]: {
              accessList: {
                [Entity.ProjectAccessList]: {},
              },
            },
          },
          members: {
            [Entity.Member]: {},
          },
          forms: {
            [Entity.Form]: {},
          },
          assets: {
            [Entity.Asset]: {
              accessList: {
                [Entity.AssetAccessList]: {},
              },
            },
          },
        },
      },
    },
  },
  countries: {
    [Entity.Country]: {},
  },
};


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

function findMatchingDocPathRegex(docPath: string) {
  for (const key in docPathsRegex) {
    if (docPathsRegex[key as Entity].test(docPath)) {
      return {entity: key as Entity, regex: docPathsRegex[key as Entity]};
    }
  }
  return {entity: null, regex: null};
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

function filterSubDocPathsByEntity(entity: Entity): string[] {
  const path = docPaths[entity];
  const paths = Object.values(docPaths);
  return paths.filter((p) => p.startsWith(path));
}

// const subPaths1 = filterSubDocPathsByEntity(Entity.Project);
// console.log(subPaths1);

function expandAndGroupDocPaths(startingDocPath: string) {
  const groupedPaths: { [key: string]: string[] } = {};
  const {entity} = findMatchingDocPathRegex(startingDocPath);
  if (!entity) {
    return groupedPaths;
  }
  const entityDocPath = docPaths[entity];
  const subDocPaths = filterSubDocPathsByEntity(entity);

  const values = Object.values(subDocPaths).map((p) => p.replace(entityDocPath, startingDocPath));
  const sortedValues = values.sort();
  const newPathMap = new Map<string, string[]>();
  const expandedPaths: string[] = [];

  while (sortedValues.length > 0) {
    const path = sortedValues.shift();
    if (!path) {
      break;
    }

    let skipPath = false;
    for (const key of [...newPathMap.keys()].sort()) {
      if (path.startsWith(key)) {
        skipPath = true;
        const values = newPathMap.get(key);
        const newPaths = (values || []).map((value) => path.replace(key, value));
        sortedValues.push(...newPaths);
        break;
      }
    }

    if (skipPath) continue;

    if (/{\w+Id}$/.test(path)) {
      // TODO:  This is a simulated fetch of data.  Do actual firebase fetch of empty documents.  Just fetch the id.
      const randomIds = Array.from({length: 3}, () => Math.floor(Math.random() * 1000));
      const newPaths = randomIds.map((id) => path.replace(/({\w+Id})$/, id.toString()));
      newPathMap.set(path, newPaths);
      sortedValues.push(...newPaths);
      continue;
    }

    expandedPaths.push(path);
  }

  // Group expandedPaths based on docPaths keys and values
  for (const [key, regex] of Object.entries(docPathsRegex)) {
    groupedPaths[key] = expandedPaths.filter((path) => path.match(regex)) as string[];
  }

  return groupedPaths;
}

// const groupedPaths = expandAndGroupDocPaths("users/12343/organizations/3214/projects/2314");
// console.log(groupedPaths);

export {Entity, docPaths, docPathsRegex, findMatchingDocPathRegex, colPaths, filterSubDocPathsByEntity, expandAndGroupDocPaths};
