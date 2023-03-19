import {docPaths, docPathsRegex} from "./index";

function findMatchingDocPathRegex(docPath: string, docPathsRegex: Record<string, RegExp>) {
  for (const key in docPathsRegex) {
    if (docPathsRegex[key].test(docPath)) {
      return {entity: key, regex: docPathsRegex[key]};
    }
  }
  return {entity: null, regex: null};
}

function filterSubDocPathsByEntity(entity: string, docPaths: Record<string, string>): string[] {
  const path = docPaths[entity];
  const paths = Object.values(docPaths);
  return paths.filter((p) => p.startsWith(path));
}

async function expandAndGroupDocPaths(startingDocPath: string, idsFetcher: (collectionPath: string) => Promise<string[]>) {
  const groupedPaths: { [key: string]: string[] } = {};
  const {entity} = findMatchingDocPathRegex(startingDocPath, docPathsRegex);
  if (!entity) {
    return groupedPaths;
  }
  const entityDocPath = docPaths[entity];
  const subDocPaths = filterSubDocPathsByEntity(entity, docPaths);

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
      const ids = await idsFetcher(collectionPath);
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
