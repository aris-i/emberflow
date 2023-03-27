import {IdGenerator, QueryCondition} from "../types";
import {docPaths, docPathsRegex} from "../index";
import {fetchIds} from "./query";

export function findMatchingDocPathRegex(docPath: string, docPathsRegex: Record<string, RegExp>) {
  for (const key in docPathsRegex) {
    if (docPathsRegex[key].test(docPath)) {
      return {entity: key, regex: docPathsRegex[key]};
    }
  }
  return {entity: null, regex: null};
}

export function filterSubDocPathsByEntity(entity: string, docPaths: Record<string, string>): string[] {
  const path = docPaths[entity];
  const paths = Object.values(docPaths);
  return paths.filter((p) => p.startsWith(path));
}

export async function expandAndGroupDocPaths(startingDocPath: string, idsFetcher: IdGenerator) {
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

export async function hydrateDocPath(destDocPath: string, entityCondition: Record<string, QueryCondition>): Promise<string[]> {
  const pathSegments = destDocPath.split("/");
  const documentPaths: string[] = [];

  // Create a queue to keep track of the remaining path segments to process
  const queue: [string[], number][] = [[pathSegments, 0]];

  // Process the queue until all path segments have been processed
  while (queue.length > 0) {
    const [segments, idx] = queue.shift()!;

    // Find the next path segment that is wrapped by curly braces
    let braceIdx = -1;
    for (let i = idx; i < segments.length; i++) {
      if (segments[i].startsWith("{")) {
        braceIdx = i;
        break;
      }
    }

    if (braceIdx === -1) {
      // We've reached the end of the path, so add it to the document paths
      documentPaths.push(segments.join("/"));
    } else {
      // Extract the collection path and fetch its IDs
      const collectionPathSegments = segments.slice(0, braceIdx);
      const collectionPath = collectionPathSegments.join("/");
      const entity = segments[braceIdx].slice(1, -3);
      // TODO: Test for entity condition
      const condition = entityCondition[entity];
      const ids = await fetchIds(collectionPath, condition);

      // Generate the document paths by merging the IDs with the collection path
      const remainingPathSegments = segments.slice(braceIdx);
      // TODO: Test for no id's returned due to condition
      for (const id of ids) {
        const documentPathSegments = [...collectionPathSegments, id, ...remainingPathSegments.slice(1)];
        queue.push([documentPathSegments, braceIdx + 1]);
      }
    }
  }

  return documentPaths;
}

