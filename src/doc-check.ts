import { execFileSync } from "node:child_process";
import {
  existsSync,
  readFileSync,
} from "node:fs";
import {
  join,
  posix,
  resolve,
} from "node:path";
import { pathToFileURL } from "node:url";
import {
  errorMessage,
  writeErrorRecord,
} from "./one-line.js";

export interface Finding {
  check: string;
  detail: string;
}

export interface DocCheckOptions {
  repoRoot?: string;
  transcripts?: Map<string, string>;
}

const REFERENCE_EXTENSIONS =
  /\.(md|ts|js|mjs|cjs|json|ps1|toml|sql)$/;

/*
 * Only the documents a reader follows to operate the thing. Design
 * notes and review transcripts describe states of the world that are
 * gone on purpose, and research notes cite other repositories; holding
 * either to "this path resolves here" would report history as breakage
 * and teach whoever runs this to ignore it.
 */
const OPERATIONAL_DOCS = [
  "README.md",
  "README.ja.md",
  "docs/deploy.md",
];

function trackedFiles(
  repoRoot: string,
): string[] {
  return execFileSync(
    "git",
    ["ls-files"],
    {
      cwd: repoRoot,
      encoding: "utf8",
    },
  )
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

/*
 * Read every document through this. A Windows checkout has CRLF, and a
 * pattern anchored on bare newlines then matches nothing, which this
 * tool reports as zero problems and zero skips: the same words as a
 * pass, from a run that looked at nothing.
 */
function readDocument(
  path: string,
): string {
  return readFileSync(path, "utf8").replace(
    /\r\n/g,
    "\n",
  );
}

function looksLikeRepoPath(
  raw: string,
  requireDirectory: boolean,
): boolean {
  const value = raw.split("#")[0] ?? "";

  return (
    (!requireDirectory ||
      value.includes("/")) &&
    !value.startsWith("http") &&
    !value.startsWith("mailto:") &&
    !value.startsWith("~") &&
    !value.startsWith("%") &&
    !value.startsWith(".") &&
    !value.includes("<") &&
    !value.includes(" ") &&
    !value.includes("\\") &&
    REFERENCE_EXTENSIONS.test(value)
  );
}

function referencesIn(
  text: string,
): string[] {
  const found = new Set<string>();

  /*
   * A link says navigate here, so a root-level target counts. Inline
   * code is a guess at what is a path, so it keeps the directory
   * requirement; the guide names `settings.json` as a file the reader
   * owns rather than one this tree ships.
   */
  for (const match of text.matchAll(
    /\[[^\]]*\]\(([^)\s]+)\)/g,
  )) {
    const value = match[1] ?? "";

    if (looksLikeRepoPath(value, false)) {
      found.add(value);
    }
  }

  for (const match of text.matchAll(
    /`([^`\n]+)`/g,
  )) {
    const value = match[1] ?? "";

    if (looksLikeRepoPath(value, true)) {
      found.add(value);
    }
  }

  return [...found];
}

export function checkReferences(
  repoRoot: string,
): Finding[] {
  const tracked = trackedFiles(repoRoot);
  const trackedSet = new Set(tracked);
  const findings: Finding[] = [];

  for (const file of tracked) {
    if (
      !OPERATIONAL_DOCS.includes(file)
    ) {
      continue;
    }

    const text = readDocument(
      join(repoRoot, file),
    );

    for (const reference of referencesIn(
      text,
    )) {
      const target = posix.normalize(
        reference.split("#")[0] ?? "",
      );

      /*
        * dist is built, not tracked, and the handout tells the reader
        * to run exactly those files.
        */
      if (
        trackedSet.has(target) ||
        target.startsWith("dist/")
      ) {
        continue;
      }

      findings.push({
        check: "references",
        detail: `${file} points at ${target}, which this tree does not carry`,
      });
    }
  }

  return findings;
}

export interface CanonicalBlock {
  name: string;
  source: string;
  body: string;
}

export function canonicalBlocks(
  repoRoot: string,
): CanonicalBlock[] {
  const blocks: CanonicalBlock[] = [];

  for (const file of trackedFiles(
    repoRoot,
  )) {
    if (!file.endsWith(".md")) {
      continue;
    }

    const text = readDocument(
      join(repoRoot, file),
    );

    for (const match of text.matchAll(
      /<!--\s*canonical:\s*([\w-]+)\s*-->\s*\n```[\w]*\n([\s\S]*?)\n```/g,
    )) {
      blocks.push({
        name: match[1] ?? "",
        source: file,
        body: match[2] ?? "",
      });
    }
  }

  return blocks;
}

export function checkTranscripts(
  repoRoot: string,
  transcripts: Map<string, string>,
): Finding[] {
  const findings: Finding[] = [];

  for (const block of canonicalBlocks(
    repoRoot,
  )) {
    const target = transcripts.get(
      block.name,
    );

    if (target === undefined) {
      findings.push({
        check: "transcripts",
        detail: `skipped ${block.name}: no target given, pass --transcript ${block.name}=<path>`,
      });
      continue;
    }

    if (!existsSync(target)) {
      findings.push({
        check: "transcripts",
        detail: `skipped ${block.name}: ${target} is not on this machine`,
      });
      continue;
    }

    const text = readDocument(target);

    if (text.includes(block.body)) {
      continue;
    }

    findings.push({
      check: "transcripts",
      detail: `${target} no longer carries the block ${block.name} verbatim from ${block.source}`,
    });
  }

  return findings;
}

export function isSkip(
  finding: Finding,
): boolean {
  return finding.detail.startsWith(
    "skipped ",
  );
}

/*
 * A control character in a command an operator copies is invisible in
 * every viewer and changes what runs. This has landed three times from
 * the same cause: a backslash escape halved on its way through a shell
 * heredoc, so `\dist\bridge-sweep.js` reached the file carrying U+0008
 * and the command pointed at a path that does not exist. One of those
 * shipped and an automated reviewer caught it. Reading the document is
 * not enough to see it, so a machine looks instead.
 */
export function checkControlCharacters(
  repoRoot: string,
): Finding[] {
  const findings: Finding[] = [];

  for (const file of OPERATIONAL_DOCS) {
    const path = join(repoRoot, file);
    if (!existsSync(path)) {
      continue;
    }

    const text = readDocument(path);
    for (
      let index = 0;
      index < text.length;
      index += 1
    ) {
      const code = text.charCodeAt(index);
      const isControl =
        (code < 32 &&
          code !== 10 &&
          code !== 9) ||
        code === 127 ||
        (code >= 128 && code <= 159);

      if (isControl) {
        const line =
          text.slice(0, index).split("\n")
            .length;
        findings.push({
          check: "control-characters",
          detail: `${file}:${line} carries U+${code
            .toString(16)
            .padStart(4, "0")}, which no reader can see and a shell will act on`,
        });
      }
    }
  }

  return findings;
}

/*
 * The one file allowed to hand a record to a stream. Everywhere else
 * goes through the emitters it exports, which escape the separators a
 * record cannot survive.
 */
const RECORD_WRITER_OWNER =
  "src/one-line.ts";

/*
 * Names only. Spelling a call out in full here would make this list
 * a hit for itself, and the check would report its own definition as
 * the defect it exists to find.
 */
const RAW_STREAM_WRITERS = [
  "console.error",
  "console.log",
  "console.warn",
  "process.stdout.write",
  "process.stderr.write",
];

/*
 * Four rounds of review found the same defect, each time in a call site
 * that reached a stream directly and escaped nothing, and each round
 * fixed the site rather than the reach. A convention that every writer
 * remember to escape is the convention that failed four times, so this
 * takes the reach away instead: a new `console.error` in `src` fails the
 * build, and the failure names the emitter to use.
 */
export function checkRecordWriters(
  repoRoot: string,
): Finding[] {
  const findings: Finding[] = [];

  for (const file of trackedFiles(
    repoRoot,
  )) {
    if (
      !file.startsWith("src/") ||
      !file.endsWith(".ts") ||
      file === RECORD_WRITER_OWNER
    ) {
      continue;
    }

    const path = join(repoRoot, file);
    if (!existsSync(path)) {
      continue;
    }

    const lines =
      readDocument(path).split("\n");

    lines.forEach((text, index) => {
      for (const writer of RAW_STREAM_WRITERS) {
        if (
          !text.includes(`${writer}(`)
        ) {
          continue;
        }

        findings.push({
          check: "record-writers",
          detail: `${file}:${
            index + 1
          } calls ${writer}() directly; a record reaches a stream through writeErrorRecord or writeOutputRecord in ${RECORD_WRITER_OWNER}, which escapes the separators that would split it`,
        });
      }
    });
  }

  return findings;
}

export function runDocCheck(
  options: DocCheckOptions = {},
): Finding[] {
  const repoRoot =
    options.repoRoot ?? process.cwd();

  return [
    ...checkReferences(repoRoot),
    ...checkControlCharacters(repoRoot),
    ...checkRecordWriters(repoRoot),
    ...checkTranscripts(
      repoRoot,
      options.transcripts ?? new Map(),
    ),
  ];
}

function parseTranscripts(
  argv: readonly string[],
): Map<string, string> {
  const transcripts = new Map<
    string,
    string
  >();

  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] !== "--transcript") {
      throw new Error(
        "usage: doc-check.js [--transcript <name>=<path>]...",
      );
    }

    const pair = argv[i + 1] ?? "";
    const split = pair.indexOf("=");

    if (split <= 0) {
      throw new Error(
        `--transcript wants <name>=<path>, got ${JSON.stringify(pair)}`,
      );
    }

    transcripts.set(
      pair.slice(0, split),
      pair.slice(split + 1),
    );
    i += 1;
  }

  return transcripts;
}

function isDirectExecution(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }

  return (
    pathToFileURL(resolve(entry)).href ===
    import.meta.url
  );
}

if (isDirectExecution()) {
  try {
    const findings = runDocCheck({
      transcripts: parseTranscripts(
        process.argv.slice(2),
      ),
    });

    for (const finding of findings) {
      writeErrorRecord(
        `${finding.check}: ${finding.detail}`,
      );
    }

    const failures =
      findings.filter(
        (finding) => !isSkip(finding),
      ).length;

    writeErrorRecord(
      `doc-check: ${failures} problem${
        failures === 1 ? "" : "s"
      }, ${
        findings.length - failures
      } skipped`,
    );

    process.exitCode =
      failures === 0 ? 0 : 1;
  } catch (error) {
    writeErrorRecord(
      `doc-check failed: ${errorMessage(error)}`,
    );
    process.exitCode = 1;
  }
}
