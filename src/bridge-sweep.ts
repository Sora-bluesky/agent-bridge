import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  BacklogCounts,
  BridgeBus,
  Role,
  UndeliveredMessage,
  UndeliveredReport,
  getBridgeDbPath,
} from "./db.js";

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

function oneLineError(
  error: unknown,
): string {
  return (
    error instanceof Error
      ? error.message
      : String(error)
  ).replace(/[\r\n]+/g, " ");
}

export function formatBacklog(
  counts: BacklogCounts,
  now: number,
): string {
  if (counts.oldestSentAt === null) {
    return `untagged:${counts.untagged},oldest:-`;
  }

  const sentAt = Date.parse(
    counts.oldestSentAt,
  );
  const age = Number.isNaN(sentAt)
    ? "?"
    : `${Math.floor((now - sentAt) / 3_600_000)}h`;

  return `untagged:${counts.untagged},oldest:${age}`;
}

const LIST_LIMIT = 5;
const SUBJECT_LIMIT = 70;

function formatAge(
  at: string,
  now: number,
): string {
  const then = Date.parse(at);
  if (Number.isNaN(then)) {
    return "?";
  }

  const minutes = Math.floor(
    (now - then) / 60_000,
  );
  return minutes < 90
    ? `${minutes}m`
    : `${Math.floor(minutes / 60)}h`;
}

function formatSubject(
  subject: string,
): string {
  const trimmed =
    subject.length > SUBJECT_LIMIT
      ? `${subject.slice(0, SUBJECT_LIMIT)}…`
      : subject;
  return JSON.stringify(trimmed);
}

/*
 * Lines a person reads, so they say what was lost rather than how many.
 * Silent when there is nothing, and explicit about what it did not list:
 * a capped list that hides its own remainder reads as completeness.
 */
export function formatUndelivered(
  role: Role,
  report: UndeliveredReport,
  now: number,
): string[] {
  const lines: string[] = [];

  const sections: Array<
    [string, UndeliveredMessage[]]
  > = [
    ["undelivered since the last sweep", report.lost],
    ["waiting past the tag TTL", report.waiting],
  ];

  if (report.lostTotal > 0) {
    lines.push(
      `agent-bridge ${role} ${report.lostTotal} undelivered in total`,
    );
  }

  for (const [label, rows] of sections) {
    if (rows.length === 0) {
      continue;
    }

    lines.push(
      `agent-bridge ${role} ${rows.length} ${label}`,
    );

    for (const row of rows.slice(0, LIST_LIMIT)) {
      lines.push(
        `  ${formatAge(row.at, now)} -> ${
          row.toTag ?? "(untagged)"
        } ${formatSubject(row.subject)}`,
      );
    }

    if (rows.length > LIST_LIMIT) {
      lines.push(
        `  (+${rows.length - LIST_LIMIT} not listed)`,
      );
    }
  }

  return lines;
}

export function runBridgeSweep(
  argv = process.argv.slice(2),
): void {
  if (argv.length !== 0) {
    throw new Error(
      "usage: bridge-sweep.js",
    );
  }

  const dbPath = getBridgeDbPath();
  const bus = BridgeBus.open(dbPath);

  try {
    const now = Date.now();
    const claude = bus.recover(
      "claude",
      now,
    );
    const codex = bus.recover(
      "codex",
      now,
    );

    console.error(
      `agent-bridge sweep db=${JSON.stringify(
        dbPath,
      )} claude=lease:${claude.leaseExpired},requeued:${claude.requeued},bounced:${claude.bounced},fallback:${claude.fallbackDemoted},${formatBacklog(
        bus.backlog("claude"),
        now,
      )} codex=lease:${codex.leaseExpired},requeued:${codex.requeued},bounced:${codex.bounced},fallback:${codex.fallbackDemoted},${formatBacklog(
        bus.backlog("codex"),
        now,
      )}`,
    );

    const since = bus.readSweepMark();

    for (const role of [
      "claude",
      "codex",
    ] as const) {
      for (const line of formatUndelivered(
        role,
        bus.undelivered(role, since, now),
        now,
      )) {
        console.error(line);
      }
    }

    bus.writeSweepMark(now);
  } finally {
    bus.close();
  }
}

if (isDirectExecution()) {
  try {
    runBridgeSweep();
  } catch (error) {
    console.error(
      `agent-bridge sweep failed: ${oneLineError(error)}`,
    );
    process.exitCode = 1;
  }
}