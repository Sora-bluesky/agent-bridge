import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import {
  BUSY_TIMEOUT_MS,
  parseRolePolicy,
  PRESENTED_TTL_MS,
  getBridgeDbPath,
} from "./db.js";

export type HookEvent =
  | "stop"
  | "user-prompt-submit";

export interface PendingCounts {
  /** Rows any session may act on: untagged mail plus every expired category. */
  fetchable: number;
  /** Live tagged rows only their addressee can claim. */
  addressed_elsewhere: number;
  stored: number;
  expired_claimed: number;
  expired_presented: number;
  expired_tagged: number;
  total: number;
  /** Whether claude-bound mail requires the reader to have declared a tag. */
  strict: boolean;
}

interface HookPayload {
  stop_hook_active?: unknown;
  [key: string]: unknown;
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

function oneLineError(
  error: unknown,
): string {
  return (
    error instanceof Error
      ? error.message
      : String(error)
  ).replace(/[\r\n]+/g, " ");
}

function parseEvent(
  argv: readonly string[],
): HookEvent {
  if (
    argv.length !== 2 ||
    argv[0] !== "--event" ||
    (argv[1] !== "stop" &&
      argv[1] !== "user-prompt-submit")
  ) {
    throw new Error(
      "usage: hook-notify.js --event stop|user-prompt-submit",
    );
  }

  return argv[1];
}

function parsePayload(raw: string): HookPayload {
  if (raw.trim().length === 0) {
    throw new Error(
      "hook stdin payload is empty",
    );
  }

  const parsed: unknown = JSON.parse(raw);
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    Array.isArray(parsed)
  ) {
    throw new Error(
      "hook stdin payload must be a JSON object",
    );
  }

  return parsed as HookPayload;
}

async function readStdin(): Promise<string> {
  process.stdin.setEncoding("utf8");

  let input = "";
  for await (const chunk of process.stdin) {
    input += chunk;
  }

  return input;
}

export function countPendingClaudeMessages(
  dbPath = getBridgeDbPath(),
  now = Date.now(),
): PendingCounts {
  const db = new Database(dbPath, {
    readonly: true,
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    db.pragma(
      `busy_timeout = ${BUSY_TIMEOUT_MS}`,
    );

    const presentedCutoff = new Date(
      now - PRESENTED_TTL_MS,
    ).toISOString();

    /*
     * stored and expired_tagged are mutually exclusive so total remains a
     * count of rows rather than a count of matching conditions.
     */
    const row = db
      .prepare(
        `SELECT
           (
             SELECT COUNT(*)
               FROM messages
              WHERE to_role = 'claude'
                AND status = 'stored'
                AND to_tag IS NULL
           ) AS stored,
           (
             SELECT COUNT(*)
               FROM messages
              WHERE to_role = 'claude'
                AND status = 'stored'
                AND to_tag IS NOT NULL
                AND (
                  tag_expires_at IS NULL
                  OR tag_expires_at >= @now
                )
           ) AS addressed_elsewhere,
           (
             SELECT COUNT(*)
               FROM messages
              WHERE to_role = 'claude'
                AND status = 'claimed'
                AND lease_expires_at < @now
           ) AS expired_claimed,
           (
             SELECT COUNT(*)
               FROM messages
              WHERE to_role = 'claude'
                AND status = 'presented'
                AND acked_at IS NULL
                AND presented_at < @presentedCutoff
           ) AS expired_presented,
           (
             SELECT COUNT(*)
               FROM messages
              WHERE to_role = 'claude'
                AND status = 'stored'
                AND to_tag IS NOT NULL
                AND tag_expires_at < @now
           ) AS expired_tagged`,
      )
      .get({
        now,
        presentedCutoff,
      }) as {
      stored: number;
      addressed_elsewhere: number;
      expired_claimed: number;
      expired_presented: number;
      expired_tagged: number;
    };

    const fetchable =
      row.stored +
      row.expired_claimed +
      row.expired_presented +
      row.expired_tagged;

    /*
     * The hook cannot see whether this session declared a tag, because
     * the declaration lives in the MCP server process and this is a
     * different one. Under strict addressing that missing bit decides
     * everything, so the count stays as it is and the notice says what
     * the reader has to check for itself.
     */
    const policy = db
      .prepare(
        "SELECT v FROM meta WHERE k = ?",
      )
      .get("strict_addressing") as
      | { v: unknown }
      | undefined;

    return {
      ...row,
      fetchable,
      total: fetchable + row.addressed_elsewhere,
      strict: parseRolePolicy(
        "strict_addressing",
        policy?.v,
      ).has("claude"),
    };
  } finally {
    db.close();
  }
}

function createNotice(
  counts: PendingCounts,
): string {
  return (
    `agent-bridgeの状況: 取得可能=${counts.fetchable}、` +
    `他セッション宛=${counts.addressed_elsewhere}` +
    `（内訳: untagged=${counts.stored}、` +
    `期限切れclaimed=${counts.expired_claimed}、` +
    `期限切れpresented=${counts.expired_presented}、` +
    `期限切れtag=${counts.expired_tagged}）。` +
    (counts.strict
      ? "strict_addressingが有効です。bridge_helloでタグを宣言していないセッションは、取得可能に数えた分も含めて何も取得できません。宣言していないなら何もせず終了してください。自分がその宛先のレーンであるときだけ、bridge_helloで宣言してから次へ進みます。"
      : "取得可能が1件以上なら、まずbridge_fetch(peek=true)を呼んでください。") +
    "引数なしのbridge_fetchを先に呼ばないでください。" +
    "peekの既定はfalseなので、その呼び出しは宛先を判断する前に最大3件をclaimし、本文を受け取ってしまいます。" +
    "peekは状態を変えず、本文も返しません。返るのはsubject・to_tag・from_tag・body_bytesです。" +
    "取得可能が0件で他セッション宛だけがある場合、" +
    "このセッションでbridge_helloによりその宛先タグを宣言していれば取得できます。" +
    "宣言していなければ何もせず終了してください（そのメールは宛先のセッションが取ります）。" +
    "自分宛と判断できた便だけ、bridge_fetch(message_id=<その ID>)で本文込みで取ります。" +
    "判断できない便は<message_id>と<subject>だけを出して次の受け手に残します。" +
    "取った便は" +
    "「📬 bridge 受信: <message_id> <subject>」の形で本文までチャットに表示し、" +
    "表示できたらすぐbridge_ackしてください。" +
    "ackは受領の確認で、作業の完了を待つものではありません。" +
    "結果は別便のbridge_sendで返します。" +
    "読み取り専用ターンではpeekだけを使い、本文の取得へ進みません。"
  );
}

export function createHookOutput(
  event: HookEvent,
  counts: PendingCounts,
): string | null {
  if (counts.total === 0) {
    return null;
  }

  const notice = createNotice(counts);

  if (event === "stop") {
    return JSON.stringify({
      decision: "block",
      reason: notice,
    });
  }

  return JSON.stringify({
    hookSpecificOutput: {
      hookEventName: "UserPromptSubmit",
      additionalContext: notice,
    },
  });
}

export async function runHookNotify(
  argv = process.argv.slice(2),
): Promise<void> {
  try {
    const event = parseEvent(argv);
    const payload = parsePayload(
      await readStdin(),
    );

    if (
      payload.stop_hook_active === true
    ) {
      return;
    }

    const output = createHookOutput(
      event,
      countPendingClaudeMessages(),
    );

    if (output !== null) {
      process.stdout.write(`${output}\n`);
    }
  } catch (error) {
    process.stderr.write(
      `agent-bridge hook skipped: ${oneLineError(error)}\n`,
    );
  }
}

if (isDirectExecution()) {
  void runHookNotify();
}
