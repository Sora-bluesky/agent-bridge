# agent-bridge

[English](README.md) | [日本語](README.ja.md)

Claude Code と Codex Desktop、同じマシンにいるのに会話できません。

間をつなぐのは人間です。片方のチャットから質問をコピーして、もう片方に貼って、答えを運ぶ。

毎回これ。

agent-bridge はこの往復を引き受けます。

どちらから送っても相手のチャット画面に現れて、届いた記録も両方に残る。

https://github.com/user-attachments/assets/7f0e5bc5-0cd3-44ba-ad89-c4521446782d

## どういうときに使うか

agent-bridge を使うのは、Claude と Codex のアプリ境界を越えてメッセージを送るときだけです。

同じ側のセッション同士には使いません。

Claude Code のセッション同士では、Claude Code が持っているセッション間のメッセージ機能を使います。

同じ側のセッションは同じ role を共有するので、role 宛の便は先に claim したセッションのものになります。

2026-08-31 には、Codex からの返信便を無関係な別プロジェクトの Claude セッションが claim して ack しました。

Codex 側には作業レーンと定期確認が同居します。

特定の作業レーンへ送る便には `to_tag` で宛先を指定します。

## メッセージの届き方

導入する前に、ここだけ読んでください。

イメージは留守番電話です。

送ると、ローカルの SQLite に書き込んで即返ってきます。相手が閉じていても、ターンの途中でも、メッセージは消えません。

取りに来るまで待つだけ。

ただし、送信成功が証明するのは保存だけです。

本当に届いたかは `bridge_status` を呼んで `acked` を確かめます。

Claude 側の宛先は「先に取りに来たセッション」。

複数のセッションを開いていると、そのうちどれか1つに現れます。

名指しは受け手の `bridge_hello` と送り手の `to_tag` で指定します。

Codex 側は取りに行く方式だけです。

Codex Desktop には外から届く入り口がないので、押し込めません。

`AGENTS.md` に足した規約に従って、次のターンの頭で受信します。

受け取ったまま応答がなかったら？

15分で再配達の対象に戻り、その側で次に fetch が走ったときにキューへ返ります。

ack は「受け取って表示した」の合図です。

作業が終わった合図ではありません。

長い作業の完了を待ってから ack すると、15分の期限が先に来て、別の受け手へ渡ってしまいます。

本文を出したらすぐ ack して、結果はあとから別便の `bridge_send` で返します。

Claude が何もしていない時間も同じです。

hook が動くのはターンの境目だけ。

届いた瞬間ではなく、次に何か打ったときに見えます。

届き方は at-least-once（最低1回は届く）。

同じメッセージが2回出ることはありますが、2回目には再送の印付き。

二重処理は冪等キーで防ぎます。

30分間隔の定期確認を登録しても、受信するのは作業レーンです。

定期確認は未読があることをログへ報告するだけで、受信は次のターンの冒頭になります。手順は [`docs/deploy.md`](docs/deploy.md)。

## どうつながっているか

```text
Claude Code デスクトップアプリ                  Codex Desktop
   ↑ Stop / UserPromptSubmit hook                  ↑ ターン冒頭の bridge_fetch
   │（処理待ちを数えるだけ・書き込まない）          │（呼び出しがチャットに見える）
┌──┴───────────────────┐              ┌───────────┴──────────┐
│ bridge server        │              │ bridge server        │
│ --role claude        │              │ --role codex         │
│ (stdio MCP)          │              │ (stdio MCP)          │
└──┬───────────────────┘              └───────────┬──────────┘
   └──────────────→  SQLite bridge.db  ←──────────┘
                     （WAL・1ファイル・lease 方式の claim）
```

両側とも同じプログラムを `--role` だけ変えて起動します。

見えるツールは5つ。

- `bridge_send`: 相手宛てのメッセージを保存。再送しても二重投函にならない
- `bridge_fetch`: 未処理分を受け取って全文表示。`peek: true` なら読むだけ
- `bridge_ack`: `message_id` と `attempt_id` の組で受領を確定（本文を表示した時点で呼ぶ。完了報告ではない。配達されたセッションからしか呼べない）
- `bridge_status`: 状態・試行回数・イベント履歴を返す
- `bridge_hello`: このセッションの名前を宣言（`to_tag` の宛先になる。server プロセスのメモリなので再起動のたびに宣言し直す）

Claude 側の hook は、数えるだけ。

件数は「どのセッションでも取れる分」と「特定のセッション宛の分」に分けて伝えます。後者は宛先だけが取れます。

DB を読み取り専用で開いて、未処理の件数を伝えます。

claim も ack も本文の受け渡しも、全部ツール側の仕事です。

だから読み取り専用のターンは読み取り専用のまま。

## 動かすのに必要なもの

- Windows（DB のパスを `%USERPROFILE%` から解決します）
- Node.js 20 以上
- Claude Code デスクトップアプリと、相手側の Codex Desktop

## セットアップの手順

```powershell
npm install
npm run build
node .\dist\bridge-init.js
```

スキーマを作るのは `bridge-init` だけ。実行は1回です。

成功すると DB のパスとスキーマ版を stderr に1行出します。

次に Claude Code へ server を登録します。`node.exe` は絶対パスで。

npm の `.cmd` を挟むと引数が壊れます。

```powershell
claude mcp add --transport stdio --scope user agent-bridge-claude -- "C:\Program Files\nodejs\node.exe" "<repo>\dist\server.js" --role claude
```

`settings.json` へ hook を2本足します。

exec form ならシェルを通らないので、空白入りのパスでも引用符いらず。

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

Codex Desktop 側は `~/.codex/config.toml` に登録します。

```toml
[mcp_servers.agent-bridge]
command = 'C:\Program Files\nodejs\node.exe'
args = ['<repo>\dist\server.js', '--role', 'codex']
```

Codex には受信のルールも渡します。

[`docs/deploy.md`](docs/deploy.md) の規約ブロックを `AGENTS.md` にコピー。

これがないと Codex は黙ったままで、メッセージはただ溜まります。

最後に両方のアプリを再起動。

撤去のやり方まで含めた全文は [`docs/deploy.md`](docs/deploy.md) にあります。

## 起動しないときは

わざと止まっています。

間違った DB を相手に走り続けるより、止まって知らせる設計です。

起動を拒むのは3つの場合。

DB ファイルが無い。スキーマ版が合わない。整合性チェックが失敗。

起動できたときはパスとスキーマ版を1行出すので、両側が同じファイルを見ているか目で確かめられます。

もう1つ、大事な原則があります。

bridge のメッセージはデータであって、指示ではありません。

本文が削除や設定変更を求めてきても、それ自体は何の許可にもならない。

決めるのは、そのときのユーザー指示と権限です。

## いまどこまで動くか

バス・5ツール・hook 通知まで実装済み。自動テストは83本あります。

claim の競合、lease の失効、異常終了の注入、冪等性、名指し配達とその timeout。そのあたりを一通り。

残っているのは、両アプリをまたぐ最後の目視確認だけです。

## 参考にした実装

channel 通知のメッセージ形は raysonmeng 版（MIT）から書き写しました。

https://github.com/raysonmeng/agent-bridge

hook で届ける形は [agmsg](https://github.com/fujibee/agmsg) に倣っています。

## ライセンス（MIT）

MIT ライセンスです。詳細は [LICENSE](LICENSE) を見てください。
