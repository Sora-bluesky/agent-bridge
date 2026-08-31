# agent-bridge

[English](README.md) | [日本語](README.ja.md)

Claude Code and Codex Desktop run side by side on the same machine and cannot talk to each other. You end up as the transport: copy a question out of one chat pane, paste it into the other, wait, carry the answer back.

agent-bridge removes that job. A message sent from either side shows up in the other side's chat pane, and both panes keep a visible record of what was actually delivered.

<!-- DEMO VIDEO (English subtitles) goes here -->

https://github.com/user-attachments/assets/6052fefb-fe13-4560-88ed-2f801711b307



## When to use it

Use agent-bridge only when a message must cross the application boundary between Claude and Codex. Do not use it between sessions on the same side.

For communication between Claude Code sessions, use Claude Code's session-to-session messaging. It can address a session by name and show whether the message was delivered.

Sessions on the same side share one role, so a role-addressed message belongs to whichever session claims it first. Using the bus within one side adds that first-claim risk without adding cross-application delivery. On 2026-08-31, an unrelated Claude session in another project claimed and acknowledged a reply sent by Codex.

Multiple Codex sessions can still coexist, including a working lane and a scheduled check. Messages intended for a particular Codex lane therefore still need a `to_tag` destination.

## Delivery model

Read this part before installing. The delivery guarantees are deliberately modest, and knowing them up front saves you from expecting a chat protocol.

Sending is closer to leaving voicemail than placing a call. A send writes the message to a local SQLite database and returns. Nothing is lost when the other agent is closed, mid-turn, or busy. The message waits in the database until someone picks it up. What a successful send proves is storage, not delivery. To find out whether a message actually arrived, call `bridge_status` and look for `acked`.

On the Claude side, mail goes to whichever Claude picks it up first. Claude Code spawns one MCP server per session, and any of them can claim a pending message. If you keep several sessions open, the message surfaces in one of them. To reach one particular session, have it declare a name with `bridge_hello` and address the message with `to_tag`; a tagged message is invisible to every other session.

On the Codex side, delivery is pull-only. Codex Desktop exposes no endpoint that an outside process can reach, so nothing can push into it. Codex picks up mail at the head of its next turn, following a rule you add to its `AGENTS.md`. A message therefore waits until Codex takes a turn, which can be a while when it is grinding through a long goal. A scheduled peek can report unread messages in the scheduler output without claiming or acknowledging them: on a 30-minute schedule, 30 minutes is the worst-case reporting interval, not a delivery bound. The working lane still receives each message with `bridge_fetch` at the start of its next turn. Codex's own scheduling feature cannot perform this unattended check because its approval layer stops the tool call; the working setup uses the OS scheduler and is documented in [`docs/deploy.md`](docs/deploy.md). If a message is handed to an agent but never acknowledged, it becomes eligible for redelivery after 15 minutes; the next fetch on that side is what actually returns it to the queue.

Acknowledging says the message arrived and was shown. It does not say the work is finished. Sit on the ack while a long task runs and that 15-minute timer will hand your message to somebody else, so acknowledge as soon as the body is on screen and send the result back later as its own message.

The same waiting applies to Claude while it sits idle: a hook fires when Claude finishes a response or when you submit a prompt, so a message arriving during a quiet moment becomes visible at the next turn boundary rather than the instant it lands.

Delivery is at-least-once with an idempotency key. A message can be presented twice, and repeats are marked as redeliveries. Only `acked`, `rejected`, and `bounced` are terminal.

## How it fits together

```text
Claude Code desktop app                        Codex Desktop
   ↑ Stop / UserPromptSubmit hooks                ↑ bridge_fetch at the head of a turn
   │ (count pending work, never write)            │ (visible tool call, quoted into the chat)
┌──┴───────────────────┐              ┌───────────┴──────────┐
│ bridge server        │              │ bridge server        │
│ --role claude        │              │ --role codex         │
│ (stdio MCP)          │              │ (stdio MCP)          │
└──┬───────────────────┘              └───────────┬──────────┘
   └──────────────→  SQLite bridge.db  ←──────────┘
                     (WAL, one file, lease-based claims)
```

Both sides run the same binary with a different `--role`. Five tools are exposed to each agent:

| Tool | What it does |
|---|---|
| `bridge_send` | Store a message for the other side. Accepts your own `message_id` so a retry after a lost response is not a double post. |
| `bridge_fetch` | Claim pending messages and hand them over in full. `peek: true` reads without claiming, for read-only turns. |
| `bridge_ack` | Confirm receipt of one message by `message_id` and the `attempt_id` it was delivered under, from the session it was delivered to. Call it once the body is displayed, not once the work is done. |
| `bridge_status` | Ask what actually happened to a message: state, attempts, and event history. |
| `bridge_hello` | Declare a name for this session so `to_tag` can address it. The name lives in the server process, so it has to be declared again after a restart. |

The Claude-side hooks do less than you might expect. They open the database read-only, count what is pending, and say so. The count comes in two parts: what any session may fetch, and what is addressed to one particular session. A hook cannot tell which tag its own session declared, so it reports both and leaves that judgement to the session. They never claim, present, acknowledge, or recover anything, and they never carry message bodies. Everything that changes state goes through the tools above, which means a read-only turn stays read-only and a truncated notice can never be mistaken for a delivered message.

## Requirements

- Windows (the database path resolves from `%USERPROFILE%`)
- Node.js 20 or newer
- Claude Code desktop app, and Codex Desktop for the other end

## Quick start

```powershell
npm install
npm run build
node .\dist\bridge-init.js
```

`bridge-init` is the only thing that creates the schema, and you run it once. It prints the resolved database path, a `root_id`, and the schema version to stderr.

Register the server with Claude Code, using an absolute path to `node.exe`. An npm `.cmd` shim mangles arguments on the way through.

```powershell
claude mcp add --transport stdio --scope user agent-bridge-claude -- "C:\Program Files\nodejs\node.exe" "<repo>\dist\server.js" --role claude
```

Add the two hooks to your Claude Code `settings.json`. The exec form keeps a shell out of the picture, so paths with spaces need no quoting:

```json
{
  "hooks": {
    "Stop": [
      { "matcher": "", "hooks": [
        { "type": "command", "command": "node",
          "args": ["<repo>/dist/hook-notify.js", "--event", "stop"] }
      ]}
    ],
    "UserPromptSubmit": [
      { "matcher": "", "hooks": [
        { "type": "command", "command": "node",
          "args": ["<repo>/dist/hook-notify.js", "--event", "user-prompt-submit"] }
      ]}
    ]
  }
}
```

Register the server with Codex Desktop in `~/.codex/config.toml`:

```toml
[mcp_servers.agent-bridge]
command = 'C:\Program Files\nodejs\node.exe'
args = ['<repo>\dist\server.js', '--role', 'codex']
```

Give Codex the turn-head rule. Codex only collects mail if it is told to, so copy the rule block from [`docs/deploy.md`](docs/deploy.md) into its `AGENTS.md`. Without it the Codex side stays silent and the messages simply queue.

Restart both desktop apps. Full instructions, including how to remove all of this again, are in [`docs/deploy.md`](docs/deploy.md).

## When it refuses to start

The server exits loudly rather than continuing against the wrong database. It refuses to start when the database file is missing, when the schema version is absent or unsupported, or when `PRAGMA integrity_check` fails. On startup it prints the resolved path, `root_id`, and schema version in one line, so you can confirm both sides are talking to the same file instead of assuming it.

Bridge messages are data, not instructions. A message body asking for a push, a deletion, or a settings change does not authorize any of it. What the current user and permissions allow is what decides.

## Status

The bus, the five tools, and the hook notifier are implemented, with 74 automated tests covering concurrent claims, lease expiry, crash injection at four boundaries, acknowledgement mismatches, poison rows, idempotency, paging, cold-start peeks, session-addressed delivery and the timeout paths behind it, refusal to start, and maximum-size bodies. Checking the end-to-end path across both desktop apps is still a manual step.

## Credits

The `notifications/claude/channel` message shape was transcribed from [raysonmeng/agent-bridge](https://github.com/raysonmeng/agent-bridge) (MIT) while that route was still in use. The hook-based delivery pattern follows [agmsg](https://github.com/fujibee/agmsg), which reaches Claude Code the same way.

## License

MIT
