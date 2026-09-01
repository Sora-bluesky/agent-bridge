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

/*
 * Both units always. An earlier version switched to hours past ninety
 * minutes, which is a boundary with nothing behind it: the same kind of
 * borrowed number this report was corrected for once already.
 */
export function formatAge(
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
  return `${Math.floor(minutes / 60)}h${minutes % 60}m`;
}

/*
 * JSON.stringify escapes quotes and the C0 range, and subject
 * normalization already flattens C0 and C1. It leaves U+2028 and U+2029,
 * which pass through both and split a line in any reader that treats
 * them as terminators.
 */
export function formatSubject(
  subject: string,
): string {
  const trimmed =
    subject.length > SUBJECT_LIMIT
      ? `${subject.slice(0, SUBJECT_LIMIT)}…`
      : subject;
  return JSON.stringify(trimmed).replace(
    /[\u2028\u2029]/g,
    (character) =>
      `\\u${character
        .charCodeAt(0)
        .toString(16)}`,
  );
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

  if (report.lostTotal > 0) {
    lines.push(
      `agent-bridge ${role} ${report.lostTotal} undelivered in total`,
    );
  }

  if (report.lostSince > 0) {
    lines.push(
      `agent-bridge ${role} ${report.lostSince} undelivered since the last sweep`,
    );

    for (const row of report.lost) {
      lines.push(
        `  ${formatAge(row.at, now)} -> ${
          row.toTag ?? "(untagged)"
        } ${formatSubject(row.subject)}`,
      );
    }

    if (report.lostSince > report.lost.length) {
      lines.push(
        `  (+${
          report.lostSince - report.lost.length
        } not listed, reported next sweep)`,
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
    const reports = (
      ["claude", "codex"] as const
    ).map((role) => ({
      role,
      report: bus.undelivered(
        role,
        since,
        LIST_LIMIT,
      ),
    }));

    for (const { role, report } of reports) {
      for (const line of formatUndelivered(
        role,
        report,
        now,
      )) {
        console.error(line);
      }
    }

    /*
     * The cursor stops at the earliest point any role left unprinted, so
     * a capped list becomes a page rather than a hole. Advancing to the
     * present would name five losses and bury the rest, which is what a
     * cap plus a forward-only cursor does on its own.
     */
    const reached = reports
      .map(({ report }) =>
        report.lost[report.lost.length - 1]
          ?.seq,
      )
      .filter(
        (seq): seq is number =>
          seq !== undefined,
      );

    const unfinished = reports
      .filter(
        ({ report }) =>
          report.lostSince >
          report.lost.length,
      )
      .map(
        ({ report }) =>
          report.lost[report.lost.length - 1]
            ?.seq,
      )
      .filter(
        (seq): seq is number =>
          seq !== undefined,
      );

    const cursor =
      unfinished.length > 0
        ? Math.min(...unfinished)
        : reached.length > 0
          ? Math.max(...reached)
          : since;

    /* Nothing new to report leaves the cursor where it was, and the
     * guard on the write makes that a no-op while the completion stamp
     * still records that the sweep ran. */
    bus.writeSweepMark(cursor ?? 0, now);
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