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
 * Applied to the finished line rather than to the fields in it. Label
 * normalization flattens C0 and C1 and leaves U+2028 and U+2029, which
 * every reader that honours them turns into a line break, so a tag or a
 * subject carrying one splits a diagnostic in two and the second half
 * reads as a log line nobody wrote.
 *
 * Escaping per field was the first shape of this, and it covered the
 * subject while the tag beside it went out raw. A line is the thing that
 * has to stay one line, so the guarantee belongs where the line is made.
 */
export function singleLine(
  text: string,
): string {
  return text.replace(
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
  const emitted: string[] = [];
  const lines = {
    push(line: string): void {
      emitted.push(singleLine(line));
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
    `[${at}] ${line}\n`,
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
    console.error(line);
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

    const reports = (
      ["claude", "codex"] as const
    ).map((role) => {
      const since = bus.readSweepMark(role);
      return {
        role,
        since,
        report: bus.undelivered(
          role,
          since,
          LIST_LIMIT,
        ),
      };
    });

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
    for (const {
      role,
      since,
      report,
    } of reports) {
      const last =
        report.lost[report.lost.length - 1]
          ?.seq;

      /*
       * A capped page stops the cursor at what it printed, so the rest
       * arrives next sweep instead of being stepped over. Nothing new
       * leaves it where it was, which the guard on the write makes a
       * no-op while the completion stamp still records the run.
       */
      bus.writeSweepMark(
        role,
        last ?? since ?? 0,
        now,
      );
    }
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
    const line = `agent-bridge sweep failed: ${oneLineError(
      error,
    )}`;
    console.error(line);

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