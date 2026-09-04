import {
  appendFileSync,
  mkdirSync,
} from "node:fs";
import {
  dirname,
  resolve,
} from "node:path";
import { pathToFileURL } from "node:url";
import {
  BacklogCounts,
  BridgeBus,
  Role,
  UndeliveredMessage,
  UndeliveredReport,
  getBridgeDbPath,
  oppositeRole,
} from "./db.js";
import {
  errorMessage,
  escapeForOneLine,
  writeErrorRecord,
} from "./one-line.js";

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

/*
 * `stuck:` rather than `untagged:`. Since v7 a bounce holds its address
 * with no deadline, so the rows nothing terminates are no longer only the
 * untagged ones, and a label that says otherwise reads as a smaller
 * problem than the one the number is about.
 */
export function formatBacklog(
  counts: BacklogCounts,
  now: number,
): string {
  if (counts.oldestSentAt === null) {
    return `stuck:${counts.stuck},oldest:-`;
  }

  const sentAt = Date.parse(
    counts.oldestSentAt,
  );
  const age = Number.isNaN(sentAt)
    ? "?"
    : `${Math.floor((now - sentAt) / 3_600_000)}h`;

  return `stuck:${counts.stuck},oldest:${age}`;
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

export function formatSubject(
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
  const emitted: string[] = [];
  const lines = {
    push(line: string): void {
      emitted.push(escapeForOneLine(line));
    },
  };

  if (report.lostTotal > 0) {
    lines.push(
      `agent-bridge ${role} ${report.lostTotal} undelivered in total`,
    );
  }

  if (report.lostSince > 0) {
    lines.push(
      `agent-bridge ${role} ${report.lostSince} undelivered not yet reported`,
    );

    /*
     * Both halves of both addresses. An address here is a role and a
     * tag together, and the two roles are never the same one: the row
     * that failed was bound for `role`, and the bounce the sweep wrote
     * for it went back to the sender, which `CHECK (from_role <>
     * to_role)` puts in the other inbox. Printing the tag alone under a
     * heading that names `role` sent an operator to declare the arrow's
     * tag on the side that cannot see it, and get nothing -- the same
     * dead end the tag was corrected for, one field over.
     */
    const bounceRole = oppositeRole(role);

    for (const row of report.lost) {
      lines.push(
        `  ${formatAge(row.at, now)} -> ${bounceRole}/${
          row.bounceToTag ?? "(untagged)"
        } (undelivered to ${role}/${
          row.deadTag ?? "(untagged)"
        }) ${formatSubject(row.subject)}`,
      );
    }

    if (report.lostSince > report.lost.length) {
      lines.push(
        `  (+${
          report.lostSince - report.lost.length
        } not listed, carried to the next sweep)`,
      );
    }
  }

  return emitted;
}

/*
 * Task Scheduler records that a task finished and discards what it wrote,
 * so a sweep registered without this runs correctly and leaves nothing a
 * person can read. The acceptance step in the deployment guide asks for
 * the sweep line in a log; this is what puts it there.
 */
export function appendLog(
  logPath: string,
  line: string,
  at = new Date().toISOString(),
): void {
  mkdirSync(dirname(logPath), {
    recursive: true,
  });
  appendFileSync(
    logPath,
    `[${at}] ${escapeForOneLine(line)}\n`,
    "utf8",
  );
}

export function parseLogPath(
  argv: readonly string[],
): string | null {
  if (argv.length === 0) {
    return null;
  }

  if (
    argv.length !== 2 ||
    argv[0] !== "--log" ||
    !argv[1]
  ) {
    throw new Error(
      "usage: bridge-sweep.js [--log <path>]",
    );
  }

  return argv[1];
}

export function runBridgeSweep(
  argv = process.argv.slice(2),
): void {
  const logPath = parseLogPath(argv);
  const stamp = new Date().toISOString();

  const emit = (line: string): void => {
    writeErrorRecord(line);
    if (logPath !== null) {
      appendLog(logPath, line, stamp);
    }
  };

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

    emit(
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

    /*
     * Reserved before anything is printed, so an overlapping sweep cannot
     * take the same page and announce it a second time.
     */
    const reports = (
      ["claude", "codex"] as const
    ).map((role) => ({
      role,
      report: bus.reserveLosses(
        role,
        LIST_LIMIT,
      ),
    }));

    for (const { role, report } of reports) {
      for (const line of formatUndelivered(
        role,
        report,
        now,
      )) {
        emit(line);
      }
    }

    /*
     * The cursor stops at the earliest point any role left unprinted, so
     * a capped list becomes a page rather than a hole. Advancing to the
     * present would name five losses and bury the rest, which is what a
     * cap plus a forward-only cursor does on its own.
     */
    /*
     * The cursor moved when the page was reserved. This records only that
     * a sweep finished, which is the question a stopped sweep and a quiet
     * one could not be told apart by.
     */
    bus.markSweepCompleted(now);
  } finally {
    bus.close();
  }
}

if (isDirectExecution()) {
  const argv = process.argv.slice(2);
  let logPath: string | null = null;

  try {
    /*
     * Parsed before the run, because the failure has to reach the same
     * place the success does. A missing or corrupt database otherwise
     * leaves the last good sweep sitting in the log as the newest thing
     * in it, which reads as a system that is still working.
     */
    logPath = parseLogPath(argv);
    runBridgeSweep(argv);
  } catch (error) {
    const line = `agent-bridge sweep failed: ${errorMessage(
      error,
    )}`;
    writeErrorRecord(line);

    if (logPath !== null) {
      try {
        appendLog(logPath, line);
      } catch {
        /* The stderr line above is all that is left to say it. */
      }
    }

    process.exitCode = 1;
  }
}