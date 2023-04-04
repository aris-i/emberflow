import {QueryCondition} from "../types";
import {fetchIds} from "./query";
import {docPaths, docPathsRegex} from "../index";

export const _mockable = {
  filterSubDocPathsByEntity: (entity: string, excludeEntities?: string[]): string[] => {
    const path = docPaths[entity];
    const paths = Object.values(docPaths);

    // Find the doc paths of the excluded entities
    const excludePaths = excludeEntities?.map((excludeEntity) => docPaths[excludeEntity]);

    return paths.filter((p) => {
      // Check if the path starts with the given entity's doc path
      const startsWithPath = p.startsWith(path);

      // Check if the path starts with any of the excluded entity's doc paths
      const startsWithExcludedPath = excludePaths?.some((excludePath) => p.startsWith(excludePath));

      return startsWithPath && !startsWithExcludedPath;
    });
  },
};

export function findMatchingDocPathRegex(docPath: string) {
  for (const key in docPathsRegex) {
    if (docPathsRegex[key].test(docPath)) {
      return {entity: key, regex: docPathsRegex[key]};
    }
  }
  return {entity: null, regex: null};
}

export function filterSubDocPathsByEntity(entity: string, excludeEntities?: string[]): string[] {
  return _mockable.filterSubDocPathsByEntity(entity, excludeEntities);
}

export async function expandAndGroupDocPaths(
  startingDocPath: string,
  entityCondition?: Record<string, QueryCondition>,
  excludeEntities?: string[]) {
  const groupedPaths: { [key: string]: string[] } = {};
  const {entity} = findMatchingDocPathRegex(startingDocPath);
  if (!entity) {
    return groupedPaths;
  }
  const entityDocPath = docPaths[entity];
  const subDocPaths = filterSubDocPathsByEntity(entity, excludeEntities);

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
      const {entity} = findMatchingDocPathRegex(path);
      const ids = await fetchIds(collectionPath, entityCondition?.[entity!]);
      const newPaths = ids.map((id) => path.replace(/{\w+Id}$/, id.toString()));
      newPathMap.set(path, newPaths);
      sortedValues.push(...newPaths);
      continue;
    }

    expandedPaths.push(path);
  }

  // Group expandedPaths based on docPaths keys and values
  for (const [key, regex] of Object.entries(docPathsRegex)) {
    const paths = expandedPaths.filter((p) => regex.test(p));
    if (!paths.length) continue;
    groupedPaths[key] = paths;
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
      const condition = entityCondition[entity];
      const ids = await fetchIds(collectionPath, condition);

      // Generate the document paths by merging the IDs with the collection path
      const remainingPathSegments = segments.slice(braceIdx);
      for (const id of ids) {
        const documentPathSegments = [...collectionPathSegments, id, ...remainingPathSegments.slice(1)];
        queue.push([documentPathSegments, braceIdx + 1]);
      }
    }
  }

  return documentPaths;
}

