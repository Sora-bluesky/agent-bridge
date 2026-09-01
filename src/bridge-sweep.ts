import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  BacklogCounts,
  BridgeBus,
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