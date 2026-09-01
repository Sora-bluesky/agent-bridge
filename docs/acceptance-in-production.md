# 配備後の受け入れ試験

> 2026-09-01。**有効化より前に書いた。** 動いている状態を見てから合格条件を書くと、通って当たり前の
> ものしか書けない。

## この文書が答える問い

「テストが通った」「マージした」「文書に書いた」のどれも、**production で効いていることの証拠では
ない**。2026-09-01 の実測では、走っている server 39本のうち**この日にマージした変更を持つものは0本**
だった。issue が全部閉じたことと、機能が動いていることは別である。

各変更について、**走っている系から1つだけ取れる観察**を定める。テストの再実行は証拠に数えない。

## 全行に共通の前提: その server が新しいこと

観察に使うセッションの server が、変更を含むビルドより後に起動していること。**これを確かめずに
取った観察は、何を観察したのか分からない。**

- server プロセスの起動時刻が `dist` のビルド時刻より後であること、または
- 起動行に該当のフィールドが出ていること（`require_tag_at_start=` / `strict_addressing_at_start=`）

掃引タスクが30分ごとに `servers built=… N/M STALE` を出す。**`all current` になるまで、下の観察は
どれも取らない。**

### `all current` はこの前提を満たさない（2026-09-01 実測）

盤は**固定した bar より後に起動したか**を見ている。bar が固定されている間にビルドが進むと、
server が古いまま `all current` と出る。実測:

```
盤の申告          : servers bar=09-01 06:26(pinned) all current
claude.exe の起動 : 09-01 10:21
dist/db.js のビルド: 09-01 11:26   ← recovery_owed を含む
```

同時刻の `bridge_fetch(peek=true)` の実応答に **`recovery_owed` が無い**。走っている server は
10:21 の dist を握っている。**鮮度計に自分の鮮度がない**という、この文書が扱っている問題そのもの。

比較する相手は bar ではなく `dist` の mtime である。

```powershell
$built = (Get-Item <repo>\dist\db.js).LastWriteTime
Get-Process claude, ChatGPT | Where-Object { $_.StartTime -lt $built }
```

**1行でも返ったら、そのプロセスの server は下の観察に使えない。**

観察に使う前に、応答そのものに新しいフィールドが出ているかを見るのがいちばん短い。
`peek` の応答に `recovery_owed` が無ければ、その server は 2026-09-01 11:26 より前である。

### 規約と機構のどちらが古くなるかは、受け手で向きが違う（同日・記録）

Codex は AGENTS.md を**アプリ起動時のスナップショット**で持ち、server は**thread ごとに起動**する。
だから Codex 側は常に「規約 ≦ 機構」で、新しい thread は機構を持ちながら規約が知らないだけになる。
壊れない向きである。

危ない向き（規約が機構より新しい）が起きるのは、**server がセッション寿命で固定される側**、つまり
Claude である。現時点で Claude の規約は AGENTS.md から自動注入されないので実害は出ていない。

転記の順序を語る前に、**受け手ごとにどちらが先に固まるか**を確かめる。同じ「スナップショット問題」でも、
固まる対象が違えば安全な向きが逆になる。

## 観察の表

| 変更 | 観察すること | 取れなかったときの扱い |
|---|---|---|
| D1 掃引の分離 | **取得済み。** 無人発火のログに `agent-bridge sweep db="…" claude=… codex=…` が出て、旧 peek のログが増えていない。2026-09-01 11:20 の行は `untagged:0,oldest:-` を含む（下の注記を読んでから使う） | 該当なし |
| #6 転記の一致 | **取得済み（2026-09-01 11:2x）。** 実物の AGENTS.md に対して `0 problems, 0 skipped`。skip が0なので、見たうえでの0である | 該当なし |
| D4 peek が body を返さない | **取得済み（2026-09-01 14:5x・observed on a server without `recovery_owed`, i.e. pre-11:26 build）。** 下に実物 | 該当なし |
| D6 単発取得の述語 | **取得済み（2026-09-01 14:56）。** 下に実物 | 該当なし |
| C4 ack の所有権 | **取得済み（2026-09-01 14:58）。** 下に実物 | 該当なし |
| D2 `require_tag` | 有効化後、`to_tag` も `broadcast` も無い送信が `tag_required` で拒否されること。同じセッションで `broadcast: true` なら通ること | 有効化前は**保留**。済にしない |
| C2 `strict_addressing` | 有効化後、未宣言セッションの `bridge_fetch` が0件で、宣言済みセッションが同じ便を取れること。あわせて hook 通知に strict の但し書きが出ること | 有効化前は**保留**。済にしない |

**D1 のビルド標識は v7 で反転した。** `untagged:0,oldest:-` が「今日のビルド」を意味したのは
2026-09-01 の時点までである。v7 はこのラベルを `stuck:` へ改名した（期限を持たない bounce 便が
untagged 以外にも滞留を作るようになったため）。**v7 の配備後にこの文字列を見たら、それは
v7 より前のビルドが走っている証拠**である。現行の標識は `stuck:0,oldest:-` で、上の行に残した
2026-09-01 の観測は、当時の標識で取れた記録としてそのまま置いてある。

### D4 の証跡（2026-09-01）

同じ便に対する peek と非 peek の応答。`68b82e89` は headless `codex exec` からの untagged 便で、
`from_tag` が null なのは規約どおり `bridge_hello` を呼んでいないためである。

peek:

```json
{"message_id":"68b82e89-…","subject":"probe: peek returns no body",
 "to_tag":null,"from_tag":null,"body_bytes":151,"redelivery":false}
```

非 peek（同じ ID）:

```json
{"message_id":"68b82e89-…","attempt_id":"023d7a2b-…","body_bytes":151,
 "body":"This message exists so a peek can be observed. …"}
```

**`body_bytes` は両方にあり、`body` は非 peek にしかない。** 151 バイトが一致している。

観測に使った server の版は、応答に `recovery_owed` が無いことで判別している（11:26 ビルドより前）。
D4 は PR #7/#8 の成果なので、この版でも成立する。**版を書かない証跡は、何を観測したか言えない。**

## 否定形を production で踏む2件

**面倒だから省く、をしない。** 省くと「試験室でしか確かめていない」状態がそこだけ残る。どちらも
2セッションが要る。

### D6: ID を知っていても他レーン宛は取れない

1. レーン A が `bridge_hello(tag: "<lane>")` を宣言する
2. ハブが `bridge_send(to_tag: "<lane>", …)` を送り、応答の `message_id` を控える
3. **宣言していない別の Codex セッション**が `bridge_fetch(message_id: "<その ID>")` を呼ぶ
4. 応答の `messages` が空であること
5. `bridge_status` でその便が `stored` のまま、`to_tag` を保持していること
6. レーン A が `bridge_fetch(message_id: "<その ID>")` を呼び、**取れること**

**6 が要る。** 4 だけだと「単発取得そのものが壊れていて誰も取れない」でも同じ結果になる。

### C4: 他セッションは ack できない

1. レーン A がターン冒頭の `bridge_fetch` で便を受け取る（`attempt_id` が応答に出る）
2. **別のセッション**が `bridge_status(message_id)` を呼び、`events` の中の `attempt_id` を読む
3. その別セッションが `bridge_ack(message_id, attempt_id)` を呼ぶ
4. 拒否されること。文言に `this process is not the one the message is currently presented to` が入る
5. `bridge_status` でその便が `presented` のまま（`acked` になっていない）こと
6. レーン A が同じ `attempt_id` で `bridge_ack` を呼び、**通ること**

**6 が要る。** 4 だけだと「ack が誰からも通らない」でも同じ結果になる。

## D6 と C4 の証跡（2026-09-01）

送信者は headless の `codex exec`。**作業レーンへ依頼する経路は外した。** 依頼した便は4時間 ack された
まま実行されず、それは規約違反ではない（issue #19）。再現性のある送信者を自前で立てるほうが、
試験の前提として正しい。`from_tag` が null なのは、規約どおり `bridge_hello` を呼んでいないためである。

### D6: ID を知っていても他レーン宛は取れない

便 `82ed2f0a`（`to_tag: agent-bridge-lane`）に対して、同じ ID で2セッションが単発取得を呼んだ。

```
apps-hub を宣言したセッション   → {"declared_tag":"apps-hub","messages":[],...}
agent-bridge-lane を宣言した側 → messages 1件、body 込みで取得
```

**否定と肯定の対で1件。** 否定だけなら「単発取得そのものが壊れていて誰も取れない」でも同じ結果になる。

### C4: 他セッションは ack できない

宛先が受け取って `presented` の状態で、別セッションが `bridge_status` から `attempt_id` と consumer を
自分で読み、`bridge_ack` を試した。**値を教えずに読ませたのは、「知っているだけでは権限にならない」が
検証したい命題だからである。**

```
拒否文言: this process is not the one the message is currently presented to
          under attempt 5bd17ebb… for role claude.
拒否後  : status=presented / acked_at=null / attempt_count=1
```

そのあと宛先が同じ `attempt_id` で ack して通った。events の連番がそれを裏づける。

```
seq 317 sent → 321 claimed → 322 presented → 323 acked
```

**322 と 323 のあいだに1行も無い。** 失敗した ack が副作用をまったく残していない。

`bridge_status` は role でも tag でも絞っていないので、他レーン宛の `attempt_id` と consumer は誰でも
読める。**それでも通らない**ことがこの試験の内容であり、漏れているのは値であって権限ではない。

## 取れなかった行の扱い

**「観察できないので済とする」を作らない。** 取れなかった行は理由つきで未観察のまま残す。理由は
次のどれかになるはずで、どれも済ではない。

- server が古い（前提を満たしていない）→ 入れ替えてからやり直す
- 有効化していない（D2 / C2）→ 有効化してからやり直す
- 2セッション目を立てられなかった（D6 / C4）→ 立てられるときにやり直す
- 観察の機会が来なかった（D4）→ probe 便を1通投函して作る

**未観察の行がある状態で「配備完了」と書かない。** 何が未観察かを併記する。

## 証拠の形

各行について、**要約ではなく実物**を残す。ツールの応答、`bridge_status` の JSON、ログの行そのもの。
「確認した」という記述は証拠に数えない。今日1日で、申告と実測が食い違った例が3件ある（旧タスクの
存在、hook の転記、走っている server の版）。
