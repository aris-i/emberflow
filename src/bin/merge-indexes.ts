#!/usr/bin/env node
import * as fs from "fs";
import * as path from "path";
import {FirestoreIndexes, mergeFirestoreIndexes} from "../utils/firestore-indexes";

const DEFAULT_TARGET = "firestore.indexes.json";
const EMPTY_INDEXES: FirestoreIndexes = {indexes: [], fieldOverrides: []};

export interface MergeCliOptions {
  command: string;
  targetPath: string;
  outPath: string;
  fragmentPath: string;
  dryRun: boolean;
}

// Path to Emberflow's shipped firestore.indexes.json fragment. When the compiled
// CLI runs from lib/bin/merge-indexes.js, the fragment sits at the package root,
// two directories up.
export function defaultFragmentPath(): string {
  return path.resolve(__dirname, "..", "..", "firestore.indexes.json");
}

export function parseArgs(argv: string[]): MergeCliOptions {
  const args = [...argv];
  const command = args.shift() ?? "";
  let targetPath = DEFAULT_TARGET;
  let outPath = "";
  let fragmentPath = "";
  let dryRun = false;

  while (args.length > 0) {
    const arg = args.shift() as string;
    switch (arg) {
    case "--path":
    case "-p":
      targetPath = args.shift() ?? targetPath;
      break;
    case "--out":
    case "-o":
      outPath = args.shift() ?? outPath;
      break;
    case "--fragment":
    case "-f":
      fragmentPath = args.shift() ?? fragmentPath;
      break;
    case "--dry-run":
      dryRun = true;
      break;
    default:
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return {
    command,
    targetPath,
    outPath: outPath || targetPath,
    fragmentPath: fragmentPath || defaultFragmentPath(),
    dryRun,
  };
}

function readIndexesFile(filePath: string, fallback?: FirestoreIndexes): FirestoreIndexes {
  if (!fs.existsSync(filePath)) {
    if (fallback) {
      return fallback;
    }
    throw new Error(`File not found: ${filePath}`);
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8")) as FirestoreIndexes;
}

// Read the target and fragment files, merge them and (unless dryRun) write the
// result back. Returns the serialized merged content.
export function runMerge(options: MergeCliOptions): string {
  const fragment = readIndexesFile(options.fragmentPath);
  const target = readIndexesFile(options.targetPath, EMPTY_INDEXES);
  const merged = mergeFirestoreIndexes(target, fragment);
  const output = `${JSON.stringify(merged, null, 2)}\n`;

  if (options.dryRun) {
    console.log(output);
    return output;
  }

  fs.writeFileSync(options.outPath, output, "utf8");
  console.log(`Merged Emberflow indexes from ${options.fragmentPath} into ${options.outPath}.`);
  return output;
}

export function printUsage(): void {
  console.log([
    "Usage: emberflow-indexes merge [options]",
    "",
    "Merge Emberflow's required Firestore indexes into your firestore.indexes.json.",
    "",
    "Options:",
    "  -p, --path <file>      Path to your firestore.indexes.json (default: firestore.indexes.json)",
    "  -o, --out <file>       Where to write the merged result (default: same as --path)",
    "  -f, --fragment <file>  Path to Emberflow's fragment (default: the shipped copy)",
    "      --dry-run          Print the merged result without writing to disk",
    "  -h, --help             Show this help",
  ].join("\n"));
}

export function main(argv: string[]): void {
  if (argv.length === 0 || argv.includes("--help") || argv.includes("-h")) {
    printUsage();
    return;
  }

  const options = parseArgs(argv);
  if (options.command !== "merge") {
    printUsage();
    throw new Error(`Unknown command: ${options.command || "(none)"}`);
  }

  runMerge(options);
}

if (require.main === module) {
  try {
    main(process.argv.slice(2));
  } catch (err) {
    console.error((err as Error).message);
    process.exit(1);
  }
}
