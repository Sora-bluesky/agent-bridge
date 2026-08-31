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

function referencesIn(
  text: string,
): string[] {
  const found = new Set<string>();

  for (const match of text.matchAll(
    /\[[^\]]*\]\(([^)\s]+)\)/g,
  )) {
    found.add(match[1] ?? "");
  }

  for (const match of text.matchAll(
    /`([^`\n]+)`/g,
  )) {
    found.add(match[1] ?? "");
  }

  return [...found].filter((raw) => {
    const value = raw.split("#")[0] ?? "";

    return (
      value.includes("/") &&
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
  });
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

    const text = readFileSync(
      join(repoRoot, file),
      "utf8",
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

    const text = readFileSync(
      join(repoRoot, file),
      "utf8",
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

    const text = readFileSync(
      target,
      "utf8",
    ).replace(/\r\n/g, "\n");

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

export function runDocCheck(
  options: DocCheckOptions = {},
): Finding[] {
  const repoRoot =
    options.repoRoot ?? process.cwd();

  return [
    ...checkReferences(repoRoot),
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
      console.error(
        `${finding.check}: ${finding.detail}`,
      );
    }

    const failures =
      findings.filter(
        (finding) => !isSkip(finding),
      ).length;

    console.error(
      `doc-check: ${failures} problem${
        failures === 1 ? "" : "s"
      }, ${
        findings.length - failures
      } skipped`,
    );

    process.exitCode =
      failures === 0 ? 0 : 1;
  } catch (error) {
    console.error(
      `doc-check failed: ${
        error instanceof Error
          ? error.message
          : String(error)
      }`,
    );
    process.exitCode = 1;
  }
}
