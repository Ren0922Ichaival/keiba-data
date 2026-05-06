# 地方競馬分析システム

ローカル競馬データ分析・AI予想・仮想ボット運用システム。

## スタック
- フロントエンド: `index.html`（Bootstrap 5 + Chart.js、localStorage: `keiba_v1`）
- バックエンド: `server.py`（Flask、http://127.0.0.1:5000）
- データ収集: GitHub Actions（`scripts/fetch_races.py` / `scripts/bot_runner.py`）
- スクレイピング先: keiba.go.jp

## データフロー
```
keiba.go.jp → fetch_races.py → data/YYYY-MM-DD.json → server.py → index.html
                                                                 ↓
                                                    bot_runner.py → bot_state.json
```

@import .claude/rules/dev-rules.md
@import .claude/rules/data-collection.md
@import .claude/rules/prediction.md
@import .claude/rules/automation.md
@import .claude/rules/decisions.md
@import .claude/rules/issues.md

---

## コーディング行動指針（Karpathy Skills）

LLMの典型的なコーディングミスを減らすためのガイドライン。

### 1. コーディング前に考える

実装前に:
- 仮定を明示する。不確かなら質問する。
- 複数の解釈がある場合は提示する。黙って選ばない。
- よりシンプルな方法があれば指摘する。
- 不明な点があれば止まって確認する。

### 2. シンプル優先

- 依頼されていない機能を追加しない。
- 一度しか使わないコードを抽象化しない。
- 依頼されていない「柔軟性」や「設定可能性」を加えない。
- 起こりえないシナリオのエラーハンドリングを書かない。
- 200行で書けるものが50行で書けるなら書き直す。

### 3. 外科的な変更

- 関係ない箇所のコード・コメント・フォーマットを「改善」しない。
- 壊れていないものをリファクタリングしない。
- 既存のスタイルに合わせる。
- 自分の変更で生じた不要なimport・変数・関数のみ削除する。

### 4. 目標駆動の実行

タスクを検証可能なゴールに変換する:
- 「バグを直す」→「再現テストを書いてからパスさせる」
- 「リファクタリング」→「前後でテストがパスすることを確認」

複数ステップのタスクでは、ステップと検証チェックを含む簡潔なプランを示す。
