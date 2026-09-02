import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import {
  type BacklogCounts,
  type BacklogRow,
  BUSY_TIMEOUT_MS,
  BridgeBus,
  DECLARED_TAG_ENV,
  type DeclaredTag,
  parseRolePolicy,
  PRESENTED_TTL_MS,
  getBridgeDbPath,
  readDeclaredTag,
} from "./db.js";
import {
  formatBacklog,
  singleLine,
} from "./bridge-sweep.js";

export {
  DECLARED_TAG_ENV,
  readDeclaredTag,
} from "./db.js";
export type { DeclaredTag } from "./db.js";

export type HookEvent =
  | "stop"
  | "user-prompt-submit";

export interface PendingCounts {
  /** Rows any session may act on: untagged mail plus every expired category. */
  fetchable: number;
  /** Live tagged rows addressed to the tag this process declared. */
  addressed_here: number;
  /** Live tagged rows only some other addressee can claim. */
  addressed_elsewhere: number;
  stored: number;
  expired_claimed: number;
  expired_presented: number;
  expired_tagged: number;
  total: number;
  /** Whether claude-bound mail requires the reader to have declared a tag. */
  strict: boolean;
  /** The address this process answers to, or null if it declared none. */
  declared_tag: string | null;
  /*
   * Set when the environment named an address that is not a tag. The
   * counts are then those of a process with no address, and the notice
   * says which of the two situations it is in.
   */
  declared_tag_unusable: string | null;
}

interface StuckNoticeState {
  backlog: BacklogCounts;
  rows: readonly BacklogRow[];
  now: number;
}

/*
 * One limit, interpolated into every call the notice spells out. The
 * follow-up call was written without it and silently fell back to the
 * default of three, so five calls reached 22 rows while the rule said 50.
 */
const PEEK_LIMIT = 10;
const PEEK_HEAD = `bridge_fetch(peek=true, limit=${PEEK_LIMIT})`;
const PEEK_NEXT = `bridge_fetch(peek=true, limit=${PEEK_LIMIT}, cursor=<その値>)`;
const STUCK_LIST_LIMIT = 3;
const BACKLOG_AGE_PREFIX = "stuck:1,oldest:";

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
  declared: DeclaredTag = readDeclaredTag(),
): PendingCounts {
  const declaredTag = declared.tag;

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
                AND to_tag IS @tag
           ) AS addressed_here,
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
                AND to_tag IS NOT @tag
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
        tag: declaredTag,
      }) as {
      stored: number;
      addressed_here: number;
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
     * The environment says which lane this process is, and bridge_hello
     * says it again to the server; the hook cannot check that the two
     * agree. So the notice still tells the reader to declare the tag
     * itself before fetching, and under strict addressing it says what
     * happens to a session that did not.
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
      /*
       * Rows for other lanes are reported but do not decide this. A
       * bounce holds its address with no deadline, so counting them
       * here left every undeclared session with a total that never
       * returned to zero and a Stop that blocked on every turn.
       */
      total: fetchable + row.addressed_here,
      strict: parseRolePolicy(
        "strict_addressing",
        policy?.v,
      ).has("claude"),
      declared_tag: declaredTag,
      declared_tag_unusable: declared.unusable,
    };
  } finally {
    db.close();
  }
}

/*
 * Conditioning on stored === 0 was a proxy for "peek will be empty" and
 * a wrong one: live tagged mail is not stored-and-untagged, so a single
 * expired row told the addressee to end its turn and, with no sweep
 * running, kept telling it that while its own mail waited. The count
 * that answers "will this session's peek be empty" is stored plus
 * addressed_here, and the split above is what makes it available; the
 * proxy is still wrong and is still not used.
 */
function recoveryOwed(
  counts: PendingCounts,
): number {
  return (
    counts.expired_claimed +
    counts.expired_presented +
    counts.expired_tagged
  );
}

function formatBacklogAge(
  sentAt: string,
  now: number,
): string {
  return formatBacklog(
    {
      stuck: 1,
      oldestSentAt: sentAt,
    },
    now,
  ).slice(BACKLOG_AGE_PREFIX.length);
}

function formatStuckNotice(
  state: StuckNoticeState | undefined,
): string {
  if (
    state === undefined ||
    state.backlog.stuck === 0
  ) {
    return "";
  }

  const oldest =
    state.backlog.oldestSentAt === null
      ? "?"
      : formatBacklogAge(
          state.backlog.oldestSentAt,
          state.now,
        );
  const named = state.rows
    .map(
      (row) =>
        `from ${
          row.from_tag ?? "無タグ"
        }（${formatBacklogAge(
          row.sent_at,
          state.now,
        )}）`,
    )
    .join(" / ");
  const remainder =
    state.backlog.stuck > STUCK_LIST_LIMIT
      ? `（+${
          state.backlog.stuck -
          STUCK_LIST_LIMIT
        } 件）`
      : "";

  return `\n${singleLine(
    `滞留（どのタイマーも動かさない行）: ${state.backlog.stuck} 件・最古 ${oldest}。${named}${remainder}`,
  )}`;
}

function createNotice(
  counts: PendingCounts,
  stuckNotice?: StuckNoticeState,
): string {
  return (
    `agent-bridgeの状況: 取得可能=${counts.fetchable}、` +
    `自分宛=${counts.addressed_here}、` +
    `他セッション宛=${counts.addressed_elsewhere}` +
    `（内訳: untagged=${counts.stored}、` +
    `期限切れclaimed=${counts.expired_claimed}、` +
    `期限切れpresented=${counts.expired_presented}、` +
    `期限切れtag=${counts.expired_tagged}）。` +
    /*
     * Said once, plainly. The reader is told two different things about
     * the same tag below -- that mail is waiting for it and that it must
     * declare the tag before it can take any -- and neither makes sense
     * without knowing which name this process answers to.
     */
    (counts.declared_tag_unusable !== null
      ? `環境変数${DECLARED_TAG_ENV}にタグとして使えない値が入っています（${counts.declared_tag_unusable}）。宛先を持たないものとして数えているので、自分宛は常に0件になります。設定を直すまで、このセッション宛の便は件数に出ません。`
      : counts.declared_tag === null
        ? `このプロセスは宛先タグを宣言していません（環境変数${DECLARED_TAG_ENV}が空）。自分宛は常に0件になります。`
        : `このプロセスの宛先タグは${JSON.stringify(
            counts.declared_tag,
          )}です（環境変数${DECLARED_TAG_ENV}）。`) +
    (counts.addressed_here > 0
      ? `自分宛の${counts.addressed_here}件は、このセッションでbridge_hello(tag=${JSON.stringify(
          counts.declared_tag,
        )})を呼んでからでないと取得できません。環境変数の宣言はserverには届いていません。`
      : "") +
    (counts.strict
      ? "strict_addressingが有効です。bridge_helloでタグを宣言していないセッションは、取得可能に数えた分も含めて何も取得できません。宣言していないなら何もせず終了してください。自分がその宛先のレーンであるときだけ、bridge_helloで宣言してから次へ進みます。"
      : "") +
    /*
     * Outside the branch on purpose. It lived inside the non-strict arm,
     * so a strict session was told to declare a tag and then given no
     * instruction to read anything, and the rest of the notice assumed a
     * peek result that never existed.
     */
    `このセッションが取得してよいなら、まず${PEEK_HEAD}を呼んでください。` +
    (recoveryOwed(counts) > 0
      ? `取得可能のうち${recoveryOwed(counts)}件は期限切れのclaimed・presented・tagで、peekには出ません。キューへ戻せるのは定期掃引(bridge-sweep)だけで、セッションからは動かせません。peekが実際に0件を返したときだけ、その件数と掃引の登録確認の依頼を報告して終了してください。peekが便を返したなら、それは通常どおり処理します。`
      : "") +
    "引数なしのbridge_fetchを先に呼ばないでください。" +
    "peekの既定はfalseなので、その呼び出しは宛先を判断する前に最大3件をclaimし、本文を受け取ってしまいます。" +
    "peekは状態を変えず、本文も返しません。返るのはsubject・to_tag・from_tag・body_bytesです。" +
    `has_more=trueなら、応答のnext_cursorを${PEEK_NEXT}へ渡して次の頁を読みます。limitを省くと既定の3件に戻り、往復あたりの取り分が減ります。` +
    "cursorを渡さずに繰り返すと、peekは状態を変えないので同じ行が返り続け、" +
    "先頭に残した便の後ろにある自分宛の便へ到達できません。" +
    `1回に読めるのは${PEEK_LIMIT}件までで、5往復してもhas_more=trueなら、その後ろは今回のターンでは読めません。` +
    "cursorは次のターンへ持ち越さず、次のターンも先頭から読み直すので、待っても解消しません。" +
    "unacked_totalと最後のnext_cursorを報告してください。" +
    /*
     * The old wording asked the reader to decide whether other lanes'
     * mail was its own. That question is now answered before the notice
     * exists -- this hook does not fire for it at all -- so the line
     * that remains says only what to do with rows that show up beside
     * the ones this session was told about.
     */
    "他セッション宛はこの通知の対象ではありません。取りにいかず、宛先のセッションに残してください。" +
    "自分宛と判断できた便だけ、bridge_fetch(message_id=<その ID>)で本文込みで取ります。" +
    "判断できない便は<message_id>と<subject>だけを出して次の受け手に残します。" +
    "取った便は" +
    "「📬 bridge 受信: <message_id> <subject>」の形で本文までチャットに表示し、" +
    "表示できたらすぐbridge_ackしてください。" +
    "ackは受領の確認で、作業の完了を待つものではありません。" +
    "結果は別便のbridge_sendで返します。" +
    "読み取り専用ターンではpeekだけを使い、本文の取得へ進みません。" +
    formatStuckNotice(stuckNotice)
  );
}

export function createHookOutput(
  event: HookEvent,
  counts: PendingCounts,
  stuckNotice?: StuckNoticeState,
): string | null {
  if (counts.total === 0) {
    return null;
  }

  const notice = createNotice(
    counts,
    stuckNotice,
  );

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

    const now = Date.now();
    const dbPath = getBridgeDbPath();
    const counts =
      countPendingClaudeMessages(
        dbPath,
        now,
      );

    /*
     * Said on stderr as well, because the notice only exists when
     * something is waiting. A lane whose variable is a typo and whose
     * inbox is empty would otherwise be told nothing at all, and would
     * find out when mail arrives and is not counted.
     */
    if (
      counts.declared_tag_unusable !== null
    ) {
      process.stderr.write(
        `agent-bridge hook: ${counts.declared_tag_unusable}\n`,
      );
    }

    let stuckNotice:
      | StuckNoticeState
      | undefined;
    if (counts.total > 0) {
      const bus = BridgeBus.open(dbPath);
      try {
        const backlog =
          bus.backlog("claude");
        stuckNotice = {
          backlog,
          rows:
            backlog.stuck > 0
              ? bus.backlogRows(
                  "claude",
                  STUCK_LIST_LIMIT,
                )
              : [],
          now,
        };
      } finally {
        bus.close();
      }
    }

    const output = createHookOutput(
      event,
      counts,
      stuckNotice,
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
