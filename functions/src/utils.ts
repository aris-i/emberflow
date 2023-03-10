import {docPaths, docPathsRegex} from "./init-db-structure";
import {Entity} from "./custom/db-structure";

function findMatchingDocPathRegex(docPath: string) {
  for (const key in docPathsRegex) {
    if (docPathsRegex[key as Entity].test(docPath)) {
      return {entity: key as Entity, regex: docPathsRegex[key as Entity]};
    }
  }
  return {entity: null, regex: null};
}

function filterSubDocPathsByEntity(entity: Entity): string[] {
  const path = docPaths[entity];
  const paths = Object.values(docPaths);
  return paths.filter((p) => p.startsWith(path));
}

async function expandAndGroupDocPaths(startingDocPath: string, idsFetcher: (collectionPath: string, count: number) => Promise<string[]>) {
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
      const idIndex = path.lastIndexOf("/");
      const collectionPath = path.substring(0, idIndex);
      const ids = await idsFetcher(collectionPath, 3);
      const newPaths = ids.map((id) => path.replace(/{\w+Id}$/, id.toString()));
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

export {expandAndGroupDocPaths, filterSubDocPathsByEntity, findMatchingDocPathRegex};
