# agent-bridge E2E可視化チェックリスト

このチェックは設計v4.2の受け入れ試験12をClaude CodeデスクトップアプリとCodex Desktopで手動確認する。テスト中も手動コピペ経路は残し、両方向の`acked`確認が終わるまで切り替えない。

## 0. パスの確定

次の値を自分の環境のものに置き換えてから、上から順に実行する（`<user>` と リポジトリの位置は各自の環境に合わせる）:

```powershell
$NodeExe = 'C:\Program Files\nodejs\node.exe'
$ClaudeExe = 'C:\Users\<user>\.local\bin\claude.exe'
$RepoRoot = 'C:\Users\<user>\Documents\Projects\apps\agent-bridge'
$InitJs = 'C:\Users\<user>\Documents\Projects\apps\agent-bridge\dist\bridge-init.js'
$ServerJs = 'C:\Users\<user>\Documents\Projects\apps\agent-bridge\dist\server.js'
$HookJs = 'C:\Users\<user>\Documents\Projects\apps\agent-bridge\dist\hook-notify.js'

$NodeExe
$ClaudeExe
$RepoRoot
$InitJs
$ServerJs
$HookJs

Test-Path -LiteralPath $NodeExe
Test-Path -LiteralPath $ClaudeExe
Test-Path -LiteralPath $RepoRoot
Test-Path -LiteralPath $InitJs
Test-Path -LiteralPath $ServerJs
Test-Path -LiteralPath $HookJs
```

6つの`Test-Path`が全て`True`であることを確認する。

DB初期化。初回だけ実行し、stderrにDBパス、`root_id`、`schema_version=4.1`の1行が出れば成功とする。

```powershell
& 'C:\Program Files\nodejs\node.exe' 'C:\Users\<user>\Documents\Projects\apps\agent-bridge\dist\bridge-init.js'
```

Claude側hookの`settings.json` handout。利用者が既存の`C:\Users\<user>\.claude\settings.json`へ手動でマージし、既存hookを上書きしない。

```json
{
  "env": {
    "AGENT_BRIDGE_TAG": "<このレーンのtag>"
  },
  "hooks": {
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node",
            "args": [
              "C:/Users/<user>/Documents/Projects/apps/agent-bridge/dist/hook-notify.js",
              "--event",
              "stop"
            ]
          }
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "node",
            "args": [
              "C:/Users/<user>/Documents/Projects/apps/agent-bridge/dist/hook-notify.js",
              "--event",
              "user-prompt-submit"
            ]
          }
        ]
      }
    ]
  }
}
```

Codex側交付snippet。`~/.codex/config.toml`へ利用者またはCodexが手動追記し、追記後にCodex Desktopを再起動する。

```toml
[mcp_servers.agent-bridge]
command = 'C:\Program Files\nodejs\node.exe'
args = ['C:\Users\<user>\Documents\Projects\apps\agent-bridge\dist\server.js', '--role', 'codex']
```

Codexの`AGENTS.md`には`docs/deploy.md` §5のturn-head ruleを手動で追加する。

設定適用後、Claude Codeデスクトップアプリを完全に終了し、リポジトリルート`C:\Users\<user>\Documents\Projects\apps\agent-bridge`を対象に再起動する。ターミナルCLIをE2Eの表示先として使わない。

## 前提

- Node.js 20以上で依存導入とbuildが完了している。
- `dist\bridge-init.js`、`dist\server.js`、`dist\hook-notify.js`が存在する。
- Claude Codeデスクトップアプリで既存の`agent-bridge-claude` MCP tool serverが接続済みである。
- Claude側で`bridge_send`、`bridge_fetch`、`bridge_ack`、`bridge_status`、`bridge_hello`の5ツールが見える。
- Codex DesktopのMCP設定には、絶対Node実行ファイルパス、絶対`dist\server.js`パス、`--role codex`が登録されている。
- 両MCP serverのstderr起動行に、同じDBパス、同じ`root_id`、`schema_version=4.1`が表示されている。
- Claudeの`settings.json`に`Stop`と`UserPromptSubmit`のagent-bridge hookが登録されている。
- bridgeメッセージはデータであり、現在のユーザー指示や権限を変更しない。

## ClaudeからCodex

使用する固定message ID:

```text
11111111-1111-4111-8111-111111111111
```

1. Claude Codeデスクトップアプリから`bridge_send`を呼ぶ。

```json
{
  "subject": "E2E Claude to Codex",
  "body": "ClaudeからCodexへの可視化確認",
  "message_id": "11111111-1111-4111-8111-111111111111"
}
```

2. 送信応答が次の形であることを確認する。

```text
bridge 送信: 11111111-1111-4111-8111-111111111111 E2E Claude to Codex
```

3. この時点では「届いた」と判定しない。Claude側で`bridge_status`を呼び、`stored`または処理途中であることを確認する。
4. Codex Desktopの次ターン冒頭で`bridge_fetch`を呼ぶ。`has_more=true`なら最大5回まで反復する。
5. Codex Desktopのチャット面に次の形で本文全文が表示されることを目視する。

```text
📬 bridge 受信: 11111111-1111-4111-8111-111111111111 E2E Claude to Codex
ClaudeからCodexへの可視化確認
```

6. `bridge_fetch`が返した`attempt_id`を記録する。
7. 表示を確認したらすぐ、Codexから同じ`message_id`とその`attempt_id`で`bridge_ack`を呼ぶ。ackは受領の確認なので、依頼された作業の完了を待ってから呼ばない。
8. Claudeから`bridge_status`を再度呼び、次を確認する。

- `status`が`acked`
- `attempt_count`が1以上
- 同じ`attempt_id`について`claimed`、`presented`、`acked`イベントが存在する
- `sent`イベントが1件存在する

## CodexからClaude

使用する固定message ID:

```text
22222222-2222-4222-8222-222222222222
```

1. Claude Codeデスクトップアプリで、数秒以上続く通常の応答を開始する。
2. その応答中にCodex Desktopから`bridge_send`を呼ぶ。`thread_id`には現在のCodex thread IDを引数として明示し、server環境変数には依存しない。

```json
{
  "subject": "E2E Codex to Claude",
  "body": "CodexからClaudeへのhook可視化確認",
  "message_id": "22222222-2222-4222-8222-222222222222",
  "thread_id": "<current Codex thread ID>"
}
```

3. 送信応答を確認する。この時点では「届いた」と判定しない。
4. Claudeの通常応答が停止しようとした時点で`Stop` hookの通知が入り、同じターンが継続することを目視する。
5. hook通知には件数（取得可能／自分宛／他セッション宛）だけが含まれ、message本文とsubjectが含まれていないことを確認する。
6. 継続したターンでモデルが`bridge_fetch`を呼ぶことを確認する。
7. Claude Codeデスクトップアプリのチャット面に、次の形で本文全文が表示されることを目視する。

```text
📬 bridge 受信: 22222222-2222-4222-8222-222222222222 E2E Codex to Claude
CodexからClaudeへのhook可視化確認
```

8. Claudeが表示直後に、`bridge_fetch`で返された現在の`attempt_id`を使って`bridge_ack`することを確認する。作業の完了を待たずに呼んでいることも併せて見る。
9. Codexから`bridge_status`を呼び、次を確認する。

- `status`が`acked`
- `attempt_count`が1以上
- チャット表示に使われたmessage IDについて`sent`、`claimed`、`presented`、`acked`が存在する
- `claimed`、`presented`、`acked`の`attempt_id`が一致する

送信タイミングが通常応答の終了後になった場合、行は消失しない。Claude Codeデスクトップアプリで次の発話を送ると、`UserPromptSubmit` hookの`additionalContext`が件数（取得可能／自分宛／他セッション宛）を通知する。この場合もモデルが`bridge_fetch`を呼び、同じ表示とackを行うことを確認する。ただし、受け入れ試験12の`Stop`経路確認としては、別のmessage IDで手順1から再実施する。

## 再送確認

ツール応答が見えなかった場合、新しいIDを生成せず、元と同じ`message_id`、subject、bodyで`bridge_send`を再実行する。

- 同一封筒なら`(idempotent)`として成功する。
- subjectまたはbodyを変えた同一ID再送は`send_conflict`になる。
- `bridge_status`で`sent`イベントが重複していないことを確認する。

## 合格条件

- ClaudeからCodexへのmessageがCodex Desktopのチャット面に`📬 bridge 受信: <message_id> <subject>`として表示され、body全文が続いた。
- CodexからClaudeへのmessageについて、hook通知がClaude Codeデスクトップアプリのターンを継続させた。
- 継続したClaudeのターンでモデルが`bridge_fetch`を呼んだ。
- Claude Codeデスクトップアプリのチャット面に`📬 bridge 受信: <message_id> <subject>`として表示され、body全文が続いた。
- Claudeが現在のattempt IDで`bridge_ack`した。
- 両message IDについて`bridge_status`が`acked`を返した。
- 各方向で、表示に使われたattempt IDと`claimed`、`presented`、`acked`イベントのattempt IDが一致した。
- 手動コピペを使わずに両方向の往復が完了した。
