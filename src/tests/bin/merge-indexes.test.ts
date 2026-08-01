import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  defaultFragmentPath,
  main,
  MergeCliOptions,
  parseArgs,
  runMerge,
} from "../../bin/merge-indexes";
import {FirestoreIndexes} from "../../utils/firestore-indexes";

const fragment: FirestoreIndexes = {
  indexes: [],
  fieldOverrides: [
    {
      collectionGroup: "computations",
      fieldPath: "createdAt",
      indexes: [
        {queryScope: "COLLECTION", order: "ASCENDING"},
        {queryScope: "COLLECTION_GROUP", order: "ASCENDING"},
      ],
    },
  ],
};

describe("defaultFragmentPath", () => {
  it("resolves to the shipped firestore.indexes.json fragment", () => {
    const fragmentPath = defaultFragmentPath();
    expect(fs.existsSync(fragmentPath)).toBe(true);
    const shipped = JSON.parse(fs.readFileSync(fragmentPath, "utf8")) as FirestoreIndexes;
    const groups = (shipped.fieldOverrides ?? []).map((o) => o.collectionGroup);
    expect(groups).toEqual(expect.arrayContaining(["computations", "executions", "processedIds"]));
  });
});

describe("parseArgs", () => {
  it("uses defaults and points out at the target path", () => {
    const options = parseArgs(["merge"]);
    expect(options.command).toBe("merge");
    expect(options.targetPath).toBe("firestore.indexes.json");
    expect(options.outPath).toBe("firestore.indexes.json");
    expect(options.fragmentPath).toBe(defaultFragmentPath());
    expect(options.dryRun).toBe(false);
  });

  it("parses long and short flags", () => {
    const options = parseArgs([
      "merge",
      "--path", "app.json",
      "-o", "out.json",
      "--fragment", "frag.json",
      "--dry-run",
    ]);
    expect(options.targetPath).toBe("app.json");
    expect(options.outPath).toBe("out.json");
    expect(options.fragmentPath).toBe("frag.json");
    expect(options.dryRun).toBe(true);
  });

  it("throws on unknown arguments", () => {
    expect(() => parseArgs(["merge", "--nope"])).toThrow("Unknown argument: --nope");
  });
});

describe("runMerge", () => {
  let tmpDir: string;
  let fragmentPath: string;
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "emberflow-indexes-"));
    fragmentPath = path.join(tmpDir, "fragment.json");
    fs.writeFileSync(fragmentPath, JSON.stringify(fragment), "utf8");
    logSpy = jest.spyOn(console, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    logSpy.mockRestore();
    fs.rmSync(tmpDir, {recursive: true, force: true});
  });

  function optionsFor(overrides: Partial<MergeCliOptions>): MergeCliOptions {
    return {
      command: "merge",
      targetPath: path.join(tmpDir, "firestore.indexes.json"),
      outPath: path.join(tmpDir, "firestore.indexes.json"),
      fragmentPath,
      dryRun: false,
      ...overrides,
    };
  }

  it("creates the target from an empty base when it does not exist", () => {
    const targetPath = path.join(tmpDir, "firestore.indexes.json");
    runMerge(optionsFor({targetPath, outPath: targetPath}));

    const written = JSON.parse(fs.readFileSync(targetPath, "utf8")) as FirestoreIndexes;
    expect(written.fieldOverrides).toEqual(fragment.fieldOverrides);
    // Trailing newline for a clean git diff.
    expect(fs.readFileSync(targetPath, "utf8").endsWith("}\n")).toBe(true);
  });

  it("merges into an existing target without dropping its own indexes", () => {
    const targetPath = path.join(tmpDir, "firestore.indexes.json");
    fs.writeFileSync(targetPath, JSON.stringify({
      indexes: [],
      fieldOverrides: [
        {
          collectionGroup: "posts",
          fieldPath: "authorId",
          indexes: [{queryScope: "COLLECTION_GROUP", order: "ASCENDING"}],
        },
      ],
    }), "utf8");

    runMerge(optionsFor({targetPath, outPath: targetPath}));

    const written = JSON.parse(fs.readFileSync(targetPath, "utf8")) as FirestoreIndexes;
    const groups = (written.fieldOverrides ?? []).map((o) => o.collectionGroup);
    expect(groups).toEqual(["posts", "computations"]);
  });

  it("does not write anything in dry-run mode", () => {
    const targetPath = path.join(tmpDir, "firestore.indexes.json");
    const output = runMerge(optionsFor({targetPath, outPath: targetPath, dryRun: true}));

    expect(fs.existsSync(targetPath)).toBe(false);
    expect(output).toContain("computations");
  });

  it("throws when the fragment file is missing", () => {
    expect(() => runMerge(optionsFor({fragmentPath: path.join(tmpDir, "missing.json")})))
      .toThrow("File not found");
  });
});

describe("main", () => {
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    logSpy = jest.spyOn(console, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  it("prints usage when called with no arguments", () => {
    main([]);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("Usage: emberflow-indexes merge"));
  });

  it("prints usage for --help", () => {
    main(["--help"]);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining("Usage: emberflow-indexes merge"));
  });

  it("throws on an unknown command", () => {
    expect(() => main(["bogus"])).toThrow("Unknown command: bogus");
  });
});
