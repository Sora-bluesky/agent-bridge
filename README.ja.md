# agent-bridge

[English](README.md) | [日本語](README.ja.md)

Claude Code と Codex Desktop、同じマシンにいるのに会話できません。

間をつなぐのは人間です。片方のチャットから質問をコピーして、もう片方に貼って、答えを運ぶ。

毎回これ。

agent-bridge はこの往復を引き受けます。

どちらから送っても、相手の次のターンの冒頭でチャット画面に現れます。渡した記録は両方に残ります。

ただし、依存する前に配達モデルを読んでください。便は送った endpoint が取るまで待ちます。宛先に期限はありません。ack も言葉の印象より弱いことしか証明しません。

https://github.com/user-attachments/assets/7f0e5bc5-0cd3-44ba-ad89-c4521446782d

## どういうときに使うか

agent-bridge を使うのは、Claude と Codex のアプリ境界を越えてメッセージを送るときだけです。

同じ側のセッション同士には使いません。

Claude Code のセッション同士では、Claude Code が持っているセッション間のメッセージ機能を使います。

同じ側のセッションは同じ role を共有するので、role 宛の便は先に claim したセッションのものになります。

2026-08-31 には、Codex からの返信便を無関係な別プロジェクトの Claude セッションが claim して ack しました。

Codex 側には作業レーンと定期確認が同居します。

特定の作業レーンへ送る便は、そのレーンの登録済みの名前を `to_endpoints` に書きます。名前は先に `bridge-init --add-endpoint` で登録します。

## メッセージの届き方

導入する前に、ここだけ読んでください。

イメージは留守番電話です。

送ると、ローカルの SQLite に書き込んで即返ってきます。証明されるのは保存だけです。

delivery は、その endpoint の server が取るまで pending のままです。宛先に期限はありません。忙しいレーンが、role の残りへ便を明け渡すことはありません。設計文書5通が、もう読んでいない送信者へ返ってきたのは、以前の期限の経路です。

届いたと言う前に、`bridge_status` でその endpoint の delivery が `confirmed` であることを確かめます。

server は `--endpoint <登録済みの名前>` で起動し、見えるのはその endpoint の便だけです。

同じ endpoint で2つ起動すると、両方から pending が見え、claim した側だけが ack できます。同じ role の別 endpoint からは見えません。

Codex 側は取りに行く方式だけです。

Codex Desktop には外から届く入り口がないので、押し込めません。

`AGENTS.md` に足した規約に従って、次のターンの頭で受信します。

受け取ったまま応答がなかったら？

15分で再配達の対象に戻り、その側で次に fetch が走ったときにキューへ返ります。

ack は「**渡された MCP プロセスが `bridge_ack` を呼んだ**」だけを意味します。

作業が終わった合図ではありません。**人が読んだ証明でもありません。** エージェントは ack してから元の作業へ戻れます。長いゴールを回しているレーンで実際にそうなります。

長い作業の完了を待ってから ack すると、15分の期限が先に来て、別の受け手へ渡ってしまいます。

本文を出したらすぐ ack して、結果はあとから別便の `bridge_send` で返します。

Claude が何もしていない時間も同じです。

hook が動くのはターンの境目だけ。

届いた瞬間ではなく、次に何か打ったときに見えます。

届き方は at-least-once（最低1回は届く）。

同じメッセージが2回出ることはありますが、2回目には再送の印付き。

二重処理は冪等キーで防ぎます。

このシステムが言えるのは3つで、言えないものが1つあります。

| `bridge_status` が返す値 | その行が記録していること |
|---|---|
| `stored` | キューにある。まだ出ていないか、回収で戻ってきたか、期限を過ぎて掃引待ちか |
| `claimed` | あるセッションが取り、サーバはまだ presented にしていない。2分 |
| `presented` | サーバが presented にした。ack まで15分 |
| `confirmed` `rejected` `bounced` `cancelled` | 終端 |

**どれもサーバがしたことの記録です。** 相手側で何が起きたかは記録していません。`presented` は応答がプロセスを出る前に書かれるので、その後に転送が落ちれば**誰も受け取っていない便が「渡した」印のまま残ります**。`acked` は presented を保持しているプロセスが `bridge_ack` を呼んだ、というだけです。**人が読んだかどうかは DB のどこにもありません。**

`bridge_status` は `deliveries` の配列を返し、トップレベルの状態は持ちません。通知は ack で終わります。答えが要る便は `expects_reply=true` で送ります。答え・断り・撤回が済むまで、送り手は送った時点から `awaiting` に、受け手は ack した時点から `owed` に、その便を見ます。どちらの一覧も `bridge_fetch` の応答に毎回入ります。終端返信は `bridge_send(in_reply_to=<依頼のid>, reply_kind=answer|decline|withdraw)` の1回で、宛先は依頼から導出し、断りは本文に理由を書き、答えと断りは ack の後に送ります。

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
│ --endpoint <name>    │              │ --endpoint <name>    │
│ (stdio MCP)          │              │ (stdio MCP)          │
└──┬───────────────────┘              └───────────┬──────────┘
   └──────────────→  SQLite bridge.db  ←──────────┘
                     （WAL・1ファイル・lease 方式の claim）
```

両側とも同じプログラムです。変えるのは `--role` と `--endpoint` です。

見えるツールは4つ。

- `bridge_send`: 相手宛てのメッセージを保存。再送しても二重投函にならない
- `bridge_fetch`: 未処理分を受け取って全文表示。`peek: true` なら読むだけ
- `bridge_ack`: `message_id` と `attempt_id` の組で受領を確定（本文を表示した時点で呼ぶ。完了報告ではない。配達されたセッションからしか呼べない）
- `bridge_status`: 状態・試行回数・イベント履歴を返す
- `bridge_status`: 宛先ごとの delivery・試行回数・イベント履歴を返す。トップレベルの状態は無い

Claude 側の hook は、数えるだけ。

件数は、この endpoint の pending と、期限切れの lease と、期限切れの presented です。他 endpoint の pending は total に入れません。名前は `AGENT_BRIDGE_ENDPOINT` から読み、未設定なら何も出しません。

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

次に Claude Code へ server を登録します。**便を受け取るプロジェクトから**実行してください。`node.exe` は絶対パスで。

npm の `.cmd` を挟むと引数が壊れます。

```powershell
node .\dist\bridge-init.js --add-endpoint claude <登録済みの名前>
node .\dist\bridge-init.js --add-endpoint codex <登録済みの名前>
claude mcp add --transport stdio --scope project agent-bridge-claude -- "C:\Program Files\nodejs\node.exe" "<repo>\dist\server.js" --role claude --endpoint <登録済みの名前>
```

**`--scope user` にしないでください。** そのマシンの全 Claude セッションが bridge を持つことになります。同じ `--endpoint` のセッションは、その endpoint の pending を claim できます。2026-08-31 に9便が失われたのはこの登録範囲が原因です。理由は配備ガイド [`docs/deploy.md`](docs/deploy.md) にあります。

**便を受け取るプロジェクトの** `.claude/settings.json` へ hook を2本足します。理由は同じです。

exec form ならシェルを通らないので、空白入りのパスでも引用符いらず。

```json
{
  "env": {
    "AGENT_BRIDGE_ENDPOINT": "<登録済みの名前>"
  },
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

`AGENT_BRIDGE_ENDPOINT` は hook が自分の endpoint を知るための1本です。hook は別プロセスなので、server の `--endpoint` は見えません。両方に同じ名前を書きます。未設定なら hook は何も出しません。取得の可否を決めるのは server 側です。

Codex Desktop 側は `~/.codex/config.toml` に登録します。

```toml
[mcp_servers.agent-bridge]
command = 'C:\Program Files\nodejs\node.exe'
args = ['<repo>\dist\server.js', '--role', 'codex', '--endpoint', '<登録済みの名前>']
```

Codex には受信のルールも渡します。

[`docs/deploy.md`](docs/deploy.md) の規約ブロックを `AGENTS.md` にコピー。

これがないと Codex は黙ったままで、メッセージはただ溜まります。

回収の掃引をタスクスケジューラへ登録します。**任意ではなく必須**です。受信規約は「まず peek、次に id 指定か10件ずつで取る」で、期限切れの lease と presented は peek に出ません。peek が0件のセッションは、回収のために取りにいきません。掃引がないと、それをキューへ戻す手がありません。**スクリプトを手で実行しても1回掃くだけで、登録はされません。** 登録手順と、実際に走ったかの確かめ方は [`docs/deploy.md`](docs/deploy.md) にあります。

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

バス・4ツール・hook 通知まで実装済み。自動テストは83本あります。

claim の競合、lease の失効、異常終了の注入、冪等性、名指し配達とその timeout。そのあたりを一通り。

残っているのは、両アプリをまたぐ最後の目視確認だけです。

## 参考にした実装

channel 通知のメッセージ形は raysonmeng 版（MIT）から書き写しました。

https://github.com/raysonmeng/agent-bridge

hook で届ける形は [agmsg](https://github.com/fujibee/agmsg) に倣っています。

## ライセンス（MIT）

MIT ライセンスです。詳細は [LICENSE](LICENSE) を見てください。
